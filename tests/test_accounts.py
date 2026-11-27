"""Account tokens and account administration, via ``httpx.MockTransport``."""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from sessionweave_client import SessionWeaveClient, SessionWeaveError


def _client(
    seen: list[httpx.Request],
    *,
    account_token: str | None = None,
    response: httpx.Response | None = None,
) -> SessionWeaveClient:
    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return response or httpx.Response(200, json={"ok": True})

    c = SessionWeaveClient(base_url="http://relay.invalid", account_token=account_token)
    c._http = httpx.AsyncClient(
        transport=httpx.MockTransport(handler), base_url="http://relay.invalid"
    )
    return c


def _body(req: httpx.Request) -> Any:
    return json.loads(req.content) if req.content else None


@pytest.mark.asyncio
async def test_create_session_sends_account_token_bearer() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="acct-tok") as c:
        await c.create_session(purpose={"kind": "x"})
    assert seen[0].headers["authorization"] == "Bearer acct-tok"


@pytest.mark.asyncio
async def test_create_session_without_account_token_sends_no_auth() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        await c.create_session()
    assert "authorization" not in seen[0].headers


@pytest.mark.asyncio
async def test_create_session_owner_account_id_with_admin_token() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="acct-tok") as c:
        await c.create_session(owner_account_id="acc_1", admin_token="adm")
    assert _body(seen[0]) == {"owner_account_id": "acc_1"}
    assert seen[0].headers["authorization"] == "Bearer adm"


@pytest.mark.asyncio
async def test_create_session_admin_token_replaces_account_token() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="acct-tok") as c:
        await c.create_session(admin_token="adm")
    assert seen[0].headers["authorization"] == "Bearer adm"


@pytest.mark.asyncio
async def test_create_session_owner_account_id_without_admin_token_raises() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="acct-tok") as c:
        with pytest.raises(ValueError, match="owner_account_id requires admin_token"):
            await c.create_session(owner_account_id="acc_1")
    assert seen == []


@pytest.mark.asyncio
async def test_account_token_is_stripped() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="  tok \n") as c:
        assert c._account_token == "tok"
    async with _client(seen, account_token="   ") as c:
        assert c._account_token is None


@pytest.mark.asyncio
async def test_env_default_account_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SW_ACCOUNT_TOKEN", "env-tok")
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        assert c._account_token == "env-tok"
    monkeypatch.setenv("SW_ACCOUNT_TOKEN", "  ")
    async with _client(seen) as c:
        assert c._account_token is None


@pytest.mark.asyncio
async def test_join_never_carries_account_token() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(
        201, json={"participant_id": "p1", "participant_token": "pt", "role": "w"}
    )
    async with _client(seen, account_token="acct-tok", response=resp) as c:
        me = await c.join("sid", join_token="jt")
        assert me.token == "pt"
    assert seen[0].headers["authorization"] == "Bearer jt"


@pytest.mark.asyncio
async def test_participant_calls_never_carry_account_token() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(200, json={"roster": []})
    async with _client(seen, account_token="acct-tok", response=resp) as c:
        from sessionweave_client import Participant

        me = Participant(c, session_id="sid", participant_id="p", token="pt", role=None)
        await me.list_participants()
    assert seen[0].headers["authorization"] == "Bearer pt"


ME_CASES: list[tuple[str, tuple[Any, ...], dict[str, Any], str, str, Any]] = [
    ("me", (), {}, "GET", "/v1/me", None),
    ("my_tokens", (), {}, "GET", "/v1/me/tokens", None),
    ("create_my_token", ("ci",), {}, "POST", "/v1/me/tokens", {"label": "ci"}),
    (
        "create_my_token",
        ("ci",),
        {"expires_at": "2027-01-01T00:00:00Z"},
        "POST",
        "/v1/me/tokens",
        {"label": "ci", "expires_at": "2027-01-01T00:00:00Z"},
    ),
    ("my_sessions", (), {}, "GET", "/v1/me/sessions", None),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "args", "kw", "method", "path", "body"), ME_CASES)
async def test_me_methods(
    name: str, args: tuple[Any, ...], kw: dict[str, Any], method: str, path: str, body: Any
) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="acct-tok") as c:
        out = await getattr(c, name)(*args, **kw)
    assert out == {"ok": True}
    req = seen[0]
    assert (req.method, req.url.path) == (method, path)
    assert req.headers["authorization"] == "Bearer acct-tok"
    assert _body(req) == body


@pytest.mark.asyncio
async def test_revoke_my_token_returns_none_and_quotes_id() -> None:
    seen: list[httpx.Request] = []
    async with _client(
        seen, account_token="acct-tok", response=httpx.Response(204)
    ) as c:
        assert await c.revoke_my_token("a b") is None
    assert seen[0].method == "DELETE"
    assert seen[0].url.raw_path == b"/v1/me/tokens/a%20b"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("name", "args"),
    [
        ("me", ()),
        ("my_tokens", ()),
        ("create_my_token", ("x",)),
        ("revoke_my_token", ("t",)),
        ("my_sessions", ()),
    ],
)
async def test_me_without_account_token_fails_before_http(
    name: str, args: tuple[Any, ...]
) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen) as c:
        with pytest.raises(ValueError, match="account_token is not set"):
            await getattr(c, name)(*args)
    assert seen == []


ADMIN_CASES: list[tuple[str, tuple[Any, ...], dict[str, Any], str, str, Any]] = [
    (
        "admin_create_account", ("adm", "Acme"), {}, "POST", "/v1/accounts",
        {"name": "Acme", "status": "active", "created_by": "admin"},
    ),
    (
        "admin_create_account", ("adm", "Acme"),
        {"contact": "a@b.c", "status": "disabled", "created_by": "ops"},
        "POST", "/v1/accounts",
        {"name": "Acme", "contact": "a@b.c", "status": "disabled", "created_by": "ops"},
    ),
    ("admin_list_accounts", ("adm",), {}, "GET", "/v1/accounts", None),
    ("admin_get_account", ("adm", "acc_1"), {}, "GET", "/v1/accounts/acc_1", None),
    (
        "admin_update_account", ("adm", "acc_1"), {"status": "disabled"},
        "PATCH", "/v1/accounts/acc_1", {"status": "disabled"},
    ),
    (
        "admin_update_account", ("adm", "acc_1"), {"name": "N", "contact": "c"},
        "PATCH", "/v1/accounts/acc_1", {"name": "N", "contact": "c"},
    ),
    ("admin_delete_account", ("adm", "acc_1"), {}, "DELETE", "/v1/accounts/acc_1", None),
    (
        "admin_create_account_token", ("adm", "acc_1", "ci"), {},
        "POST", "/v1/accounts/acc_1/tokens", {"label": "ci"},
    ),
    (
        "admin_create_account_token", ("adm", "acc_1", "ci"),
        {"expires_at": "2027-01-01T00:00:00Z"},
        "POST", "/v1/accounts/acc_1/tokens",
        {"label": "ci", "expires_at": "2027-01-01T00:00:00Z"},
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("name", "args", "kw", "method", "path", "body"), ADMIN_CASES)
async def test_admin_methods(
    name: str, args: tuple[Any, ...], kw: dict[str, Any], method: str, path: str, body: Any
) -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, account_token="acct-tok") as c:
        out = await getattr(c, name)(*args, **kw)
    assert out == {"ok": True}
    req = seen[0]
    assert (req.method, req.url.path) == (method, path)
    # The explicit admin token is used, never the configured account token.
    assert req.headers["authorization"] == "Bearer adm"
    assert _body(req) == body


@pytest.mark.asyncio
async def test_admin_revoke_account_token_204_and_quoting() -> None:
    seen: list[httpx.Request] = []
    async with _client(seen, response=httpx.Response(204)) as c:
        assert await c.admin_revoke_account_token("adm", "a 1", "t 2") is None
    assert seen[0].method == "DELETE"
    assert seen[0].url.raw_path == b"/v1/accounts/a%201/tokens/t%202"


@pytest.mark.asyncio
async def test_admin_error_raises_sdk_error() -> None:
    seen: list[httpx.Request] = []
    resp = httpx.Response(404, json={"error": {"code": "account_not_found", "message": "x"}})
    async with _client(seen, response=resp) as c:
        with pytest.raises(SessionWeaveError):
            await c.admin_get_account("adm", "nope")


@pytest.mark.asyncio
async def test_explicit_empty_account_token_does_not_fall_back_to_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("SW_ACCOUNT_TOKEN", "env-tok")
    for explicit in ("", "  "):
        seen: list[httpx.Request] = []
        async with _client(seen, account_token=explicit) as c:
            await c.create_session()
        assert "authorization" not in seen[0].headers
    seen = []
    async with _client(seen) as c:
        await c.create_session()
    assert seen[0].headers["authorization"] == "Bearer env-tok"
