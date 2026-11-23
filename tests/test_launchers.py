"""Agent discovery, launch and coordinator calls, via ``httpx.MockTransport``."""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from sessionweave_client import SessionWeaveClient, SessionWeaveError
from sessionweave_client.client import Participant

COORD_MSG = "only the session's coordinator can request agents — ask the coordinator"


def _setup(
    seen: list[httpx.Request], *, response: httpx.Response | None = None
) -> tuple[SessionWeaveClient, Participant]:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return response or httpx.Response(200, json={"ok": True})

    c = SessionWeaveClient(base_url="http://relay.invalid")
    c._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://relay.invalid"
    )
    p = Participant(c, session_id="s1", participant_id="p1", token="tok", role="worker")
    return c, p


def _body(req: httpx.Request) -> Any:
    return json.loads(req.content) if req.content else None


@pytest.mark.asyncio
async def test_discover_agents_query_and_auth() -> None:
    seen: list[httpx.Request] = []
    c, p = _setup(seen)
    async with c:
        await p.discover_agents("web", role="coder", capability="git")
        await p.discover_agents("web")
    assert seen[0].method == "GET" and seen[0].url.path == "/v1/agents"
    assert dict(seen[0].url.params) == {
        "session_id": "s1", "domain": "web", "role": "coder", "capability": "git"
    }
    assert seen[0].headers["authorization"] == "Bearer tok"
    assert dict(seen[1].url.params) == {"session_id": "s1", "domain": "web"}


@pytest.mark.asyncio
async def test_list_domains() -> None:
    seen: list[httpx.Request] = []
    c, p = _setup(seen)
    async with c:
        await p.list_domains()
    assert seen[0].url.path == "/v1/agents/domains"
    assert dict(seen[0].url.params) == {"session_id": "s1"}


@pytest.mark.asyncio
async def test_launch_agent_bodies_and_status() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(202, json={"launch_id": "lch_1", "status": "pending"})
    c, p = _setup(seen, response=resp)
    async with c:
        out = await p.launch_agent(task="do it", domain="web", role="coder")
        await p.launch_agent(task="t", launcher_id="L", profile_id="P")
        await p.launch_status("lch/1")
    assert out == {"launch_id": "lch_1", "status": "pending"}
    assert seen[0].method == "POST" and seen[0].url.raw_path == b"/v1/sessions/s1/agents"
    assert _body(seen[0]) == {"task": "do it", "domain": "web", "role": "coder"}
    assert _body(seen[1]) == {"task": "t", "launcher_id": "L", "profile_id": "P"}
    assert seen[2].method == "GET"
    assert seen[2].url.raw_path == b"/v1/sessions/s1/agents/lch%2F1"
    assert seen[2].headers["authorization"] == "Bearer tok"


@pytest.mark.asyncio
async def test_coordinator_only_refusal_maps_to_error() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(
        403, json={"error": {"code": "permission_denied", "message": COORD_MSG}}
    )
    c, p = _setup(seen, response=resp)
    async with c:
        with pytest.raises(SessionWeaveError) as ei:
            await p.launch_agent(task="x", domain="d", role="r")
    assert ei.value.code == "permission_denied" and ei.value.status == 403
    assert COORD_MSG in ei.value.message


@pytest.mark.asyncio
async def test_set_coordinator() -> None:
    seen: list[httpx.Request] = []
    c, _p = _setup(seen)
    async with c:
        await c.set_coordinator("s1", "p2", "creator-tok")
        await c.set_coordinator("s1", None, "creator-tok")
    assert seen[0].method == "PUT" and seen[0].url.raw_path == b"/v1/sessions/s1/coordinator"
    assert _body(seen[0]) == {"participant_id": "p2"}
    assert _body(seen[1]) == {"participant_id": None}
    assert seen[0].headers["authorization"] == "Bearer creator-tok"


def test_version_matches_the_package() -> None:
    import tomllib
    from pathlib import Path

    import sessionweave_client

    pyproject = Path(__file__).resolve().parents[1] / "pyproject.toml"
    version = tomllib.loads(pyproject.read_text())["project"]["version"]
    assert sessionweave_client.__version__ == version
