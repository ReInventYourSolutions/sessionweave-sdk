"""End-to-end smoke test: host + worker in one process, no LLM.

Run against any reachable relay:

    SW_BASE=https://relay.sessionweave.com python 01_smoke.py
    SW_BASE=http://127.0.0.1:8765         python 01_smoke.py

The script exits 0 on success and prints each event it observes.
"""
from __future__ import annotations

import asyncio
import os

from sessionweave_client import SessionWeaveClient


async def main() -> None:
    base = os.environ.get("SW_BASE", "https://relay.sessionweave.com")
    async with SessionWeaveClient(base_url=base) as client:
        info = await client.info()
        print(f"relay {info['name']} v{info['version']} transports={info['transports']}")

        sess = await client.create_session(
            purpose={"kind": "sdk-smoke", "instructions": "echo what you receive"}
        )
        sid = sess["session_id"]
        print(f"created session {sid}")

        host = await client.join(sid, role="host", capabilities=["broadcast"])
        worker = await client.join(sid, role="worker", capabilities=["respond"])

        cmd = await host.send(
            payload={"q": "ping"},
            dispatch="specific",
            target={"participant_ids": [worker.participant_id]},
            timeout_seconds=30,
        )
        cid = cmd["command_id"]
        print(f"host dispatched {cid}")

        # Worker pulls and responds.
        for evt in await worker.drain_inbox():
            print(f"  worker saw {evt.name}")
            if evt.name == "command.received":
                await worker.respond(
                    evt.payload["command_id"], payload={"a": "pong"}, status="ok"
                )
                print(f"  worker responded to {evt.payload['command_id']}")

        # Give the relay a tick to fan out the result and command.completed.
        await asyncio.sleep(0.5)

        for evt in await host.drain_inbox():
            print(f"  host saw {evt.name}")

        await worker.leave(feedback={
            "summary": "echo round trip clean",
            "rating": 5,
            "issues": [],
            "metrics": {"commands_received": 1, "commands_responded": 1},
        })
        await host.leave(feedback={
            "summary": "demo host done",
            "rating": 5,
            "issues": [],
        })
        print("done")


if __name__ == "__main__":
    asyncio.run(main())
