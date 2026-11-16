"""transport= / ws_connect= injection (0.11.1)."""
from __future__ import annotations

import httpx
from websockets.exceptions import ConnectionClosedOK

from sessionweave_client import SessionWeaveClient
from sessionweave_client.client import Participant


async def test_custom_transport_is_used() -> None:
    seen: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(str(request.url))
        return httpx.Response(200, json={"version": "x"})

    async with SessionWeaveClient(
        "http://broker.test", account_token="", transport=httpx.MockTransport(handler)
    ) as c:
        assert (await c.info())["version"] == "x"
    assert seen == ["http://broker.test/v1/info"]


async def test_custom_ws_connect_is_used_for_events() -> None:
    urls: list[str] = []

    class FakeWS:
        async def __aenter__(self) -> FakeWS:
            return self

        async def __aexit__(self, *a: object) -> None:
            return None

        async def recv(self) -> str:
            raise ConnectionClosedOK(None, None)

    def connect(url: str) -> FakeWS:
        urls.append(url)
        return FakeWS()

    client = SessionWeaveClient("http://broker.test", account_token="", ws_connect=connect)
    me = Participant(client, session_id="s1", participant_id="p", token="tok", role=None)
    async with me.events() as stream:
        assert [e async for e in stream] == []
    assert urls == ["ws://broker.test/v1/sessions/s1/participants/me/events?token=tok"]
    await client.aclose()
