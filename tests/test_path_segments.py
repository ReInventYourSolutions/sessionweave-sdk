"""Every caller value placed in a URL path is validated by ``_seg``: empty,
dots-only (``.``/``..``, which httpx would collapse) and ``/`` raise before any
request; normal ids still percent-encode."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import httpx
import pytest

from sessionweave_client import Participant, SessionWeaveClient

BAD = ["", ".", "..", "...", "a/b", "/", "../x", "x/.."]
Call = Callable[[SessionWeaveClient, str], Awaitable[Any]]


def _client(seen: list[httpx.Request]) -> SessionWeaveClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    c = SessionWeaveClient(base_url="http://relay.invalid", account_token="acct")

    async def no_ws(*a: Any, **k: Any) -> Any:
        raise AssertionError("websocket opened")

    c._ws_connect = no_ws
    c._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://relay.invalid"
    )
    return c


def _p(c: SessionWeaveClient, sid: str) -> Participant:
    return Participant(c, session_id=sid, participant_id="p1", token="t", role=None)


async def _events(p: Participant) -> None:
    async with p.events():
        pass


# (name, call(client, bad), expected raw path template with {} for the id, method)
CLIENT_CALLS: list[tuple[str, Call, str]] = [
    ("revoke_my_token", lambda c, x: c.revoke_my_token(x), "/v1/me/tokens/{}"),
    ("my_token_networks", lambda c, x: c.my_token_networks(x), "/v1/me/tokens/{}/networks"),
    (
        "set_my_token_networks",
        lambda c, x: c.set_my_token_networks(x, network_policy="any"),
        "/v1/me/tokens/{}/networks",
    ),
    (
        "approve_my_network",
        lambda c, x: c.approve_my_network(x),
        "/v1/me/network-approvals/{}/approve",
    ),
    ("deny_my_network", lambda c, x: c.deny_my_network(x), "/v1/me/network-approvals/{}/deny"),
    ("admin_get_account", lambda c, x: c.admin_get_account("adm", x), "/v1/accounts/{}"),
    (
        "admin_update_account",
        lambda c, x: c.admin_update_account("adm", x, name="n"),
        "/v1/accounts/{}",
    ),
    ("admin_delete_account", lambda c, x: c.admin_delete_account("adm", x), "/v1/accounts/{}"),
    (
        "admin_create_account_token",
        lambda c, x: c.admin_create_account_token("adm", x, "lbl"),
        "/v1/accounts/{}/tokens",
    ),
    (
        "admin_token_networks",
        lambda c, x: c.admin_token_networks("adm", x, "t1"),
        "/v1/accounts/{}/tokens/t1/networks",
    ),
    (
        "admin_token_networks(token)",
        lambda c, x: c.admin_token_networks("adm", "a1", x),
        "/v1/accounts/a1/tokens/{}/networks",
    ),
    (
        "admin_set_token_networks",
        lambda c, x: c.admin_set_token_networks("adm", x, "t1", network_policy="any"),
        "/v1/accounts/{}/tokens/t1/networks",
    ),
    (
        "admin_set_token_networks(token)",
        lambda c, x: c.admin_set_token_networks("adm", "a1", x, network_policy="any"),
        "/v1/accounts/a1/tokens/{}/networks",
    ),
    (
        "admin_network_approvals",
        lambda c, x: c.admin_network_approvals("adm", x),
        "/v1/accounts/{}/network-approvals",
    ),
    (
        "admin_approve_network(account)",
        lambda c, x: c.admin_approve_network("adm", x, "ap1"),
        "/v1/accounts/{}/network-approvals/ap1/approve",
    ),
    (
        "admin_approve_network(approval)",
        lambda c, x: c.admin_approve_network("adm", "a1", x),
        "/v1/accounts/a1/network-approvals/{}/approve",
    ),
    (
        "admin_deny_network(account)",
        lambda c, x: c.admin_deny_network("adm", x, "ap1"),
        "/v1/accounts/{}/network-approvals/ap1/deny",
    ),
    (
        "admin_deny_network(approval)",
        lambda c, x: c.admin_deny_network("adm", "a1", x),
        "/v1/accounts/a1/network-approvals/{}/deny",
    ),
    (
        "admin_revoke_account_token(account)",
        lambda c, x: c.admin_revoke_account_token("adm", x, "t1"),
        "/v1/accounts/{}/tokens/t1",
    ),
    (
        "admin_revoke_account_token(token)",
        lambda c, x: c.admin_revoke_account_token("adm", "a1", x),
        "/v1/accounts/a1/tokens/{}",
    ),
    ("delete_session", lambda c, x: c.delete_session(x, "tok"), "/v1/sessions/{}"),
    ("update_purpose", lambda c, x: c.update_purpose(x, "tok", {}), "/v1/sessions/{}/purpose"),
    ("mint_join_token", lambda c, x: c.mint_join_token(x, "tok"), "/v1/sessions/{}/join-tokens"),
    (
        "mint_supervisor_token",
        lambda c, x: c.mint_supervisor_token(x, "tok"),
        "/v1/sessions/{}/supervisor-tokens",
    ),
    (
        "list_supervisor_tokens",
        lambda c, x: c.list_supervisor_tokens(x, "tok"),
        "/v1/sessions/{}/supervisor-tokens",
    ),
    (
        "revoke_supervisor_token(session)",
        lambda c, x: c.revoke_supervisor_token(x, "tok", "t1"),
        "/v1/sessions/{}/supervisor-tokens/t1",
    ),
    (
        "revoke_supervisor_token(token)",
        lambda c, x: c.revoke_supervisor_token("s1", "tok", x),
        "/v1/sessions/s1/supervisor-tokens/{}",
    ),
    (
        "post_order",
        lambda c, x: c.post_order(x, "tok", kind="pause"),  # type: ignore[arg-type]
        "/v1/sessions/{}/supervisor/notes",
    ),
    (
        "remove_participant(session)",
        lambda c, x: c.remove_participant(x, "tok", "p1"),
        "/v1/sessions/{}/participants/p1",
    ),
    (
        "remove_participant(participant)",
        lambda c, x: c.remove_participant("s1", "tok", x),
        "/v1/sessions/s1/participants/{}",
    ),
    (
        "set_coordinator",
        lambda c, x: c.set_coordinator(x, "p1", "tok"),
        "/v1/sessions/{}/coordinator",
    ),
    ("access", lambda c, x: c.access(x, "tok"), "/v1/sessions/{}/access"),
    ("get_events", lambda c, x: c.get_events(x, token="tok"), "/v1/sessions/{}/events"),
    ("describe", lambda c, x: c.describe(x, token="tok"), "/v1/sessions/{}"),
    (
        "join",
        lambda c, x: c.join(x, join_token="jt"),
        "/v1/sessions/{}/participants",
    ),
]

PARTICIPANT_CALLS: list[tuple[str, Callable[[Participant], Awaitable[Any]], str]] = [
    ("list_participants", lambda p: p.list_participants(), "/v1/sessions/{}/participants"),
    ("update_self", lambda p: p.update_self(role="r"), "/v1/sessions/{}/participants/me"),
    ("send", lambda p: p.send(payload={}), "/v1/sessions/{}/commands"),
    ("leave", lambda p: p.leave(), "/v1/sessions/{}/participants/me"),
    ("drain_inbox", lambda p: p.drain_inbox(), "/v1/sessions/{}/participants/me/inbox/drain"),
    ("launch_agent", lambda p: p.launch_agent(task="t"), "/v1/sessions/{}/agents"),
    ("events", lambda p: _events(p), "/v1/sessions/{}/participants/me/events"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", BAD)
@pytest.mark.parametrize("name,call,tmpl", CLIENT_CALLS, ids=[c[0] for c in CLIENT_CALLS])
async def test_client_rejects_bad_segment(name: str, call: Call, tmpl: str, bad: str) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        with pytest.raises(ValueError, match="path segment"):
            await call(c, bad)
    assert seen == []


@pytest.mark.asyncio
@pytest.mark.parametrize("name,call,tmpl", CLIENT_CALLS, ids=[c[0] for c in CLIENT_CALLS])
async def test_client_normal_id_encodes(name: str, call: Call, tmpl: str) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        try:
            await call(c, "a b.c..d")
        except Exception:  # response shape is irrelevant here, only the path
            pass
    assert seen and seen[0].url.raw_path.decode().split("?")[0] == tmpl.format("a%20b.c..d")


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", BAD)
@pytest.mark.parametrize("name,call,tmpl", PARTICIPANT_CALLS, ids=[c[0] for c in PARTICIPANT_CALLS])
async def test_participant_rejects_bad_session_id(
    name: str, call: Callable[[Participant], Awaitable[Any]], tmpl: str, bad: str
) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        with pytest.raises(ValueError, match="path segment"):
            await call(_p(c, bad))
    assert seen == []


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", BAD)
async def test_participant_rejects_bad_ids_in_path(bad: str) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        p = _p(c, "s1")
        for coro in (
            p.respond(bad, payload={}),
            p.reply_to_note(bad, "m"),
            p.launch_status(bad),
        ):
            with pytest.raises(ValueError, match="path segment"):
                await coro
    assert seen == []


@pytest.mark.asyncio
async def test_participant_normal_ids_encode() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        p = _p(c, "s 1")
        for coro in (
            p.respond("c.1", payload={}),
            p.reply_to_note("n 1", "m"),
            p.launch_status("l..1"),
        ):
            try:
                await coro
            except Exception:
                pass
    assert [r.url.raw_path.decode() for r in seen] == [
        "/v1/sessions/s%201/commands/c.1/responses",
        "/v1/sessions/s%201/supervisor/notes/n%201/replies",
        "/v1/sessions/s%201/agents/l..1",
    ]
