"""A SessionWeave worker driven by the Anthropic Python SDK.

The worker subscribes to a session over the WebSocket events stream
and, for every ``command.received`` event whose payload contains a
``prompt`` field, runs a Claude completion and returns the model's
response over ``session_respond``.

Run:

    pip install anthropic sessionweave-client
    export ANTHROPIC_API_KEY=...
    export SW_SESSION_ID=<sid>             # the session you want to join
    export SW_JOIN_TOKEN=<jt>              # join token from the invite link (?jt=)
    export SW_BASE=https://relay.sessionweave.com  # optional override
    python 02_anthropic_worker.py

The host side can be any other SessionWeave participant that calls
``send`` with a payload ``{"prompt": "..."}``. It receives the model's
text in the worker's ``payload`` field of the result event.
"""
from __future__ import annotations

import asyncio
import os

from sessionweave_client import SessionWeaveClient


async def main() -> None:
    try:
        from anthropic import AsyncAnthropic
    except ImportError as exc:  # pragma: no cover - optional dep
        raise SystemExit(
            "this example requires `pip install anthropic`"
        ) from exc

    sid = os.environ["SW_SESSION_ID"]
    base = os.environ.get("SW_BASE", "https://relay.sessionweave.com")
    model = os.environ.get("ANTHROPIC_MODEL", "claude-opus-4-7")

    anthropic = AsyncAnthropic()  # picks up ANTHROPIC_API_KEY

    async with SessionWeaveClient(base_url=base) as client:
        me = await client.join(
            sid,
            join_token=os.environ["SW_JOIN_TOKEN"],
            role="anthropic-worker",
            objective=f"answer prompts using {model}",
            capabilities=["respond", "llm:anthropic"],
        )
        print(f"joined session {sid} as {me.participant_id}")

        try:
            async with me.events() as stream:
                async for evt in stream:
                    if evt.name != "command.received":
                        continue
                    cid = evt.payload["command_id"]
                    prompt = evt.payload["payload"].get("prompt")
                    if not prompt:
                        await me.respond(
                            cid,
                            status="error",
                            payload={"error": "missing 'prompt' in command payload"},
                        )
                        continue

                    print(f"  responding to {cid} via {model}")
                    completion = await anthropic.messages.create(
                        model=model,
                        max_tokens=1024,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    text = "".join(
                        block.text  # type: ignore[union-attr]
                        for block in completion.content
                        if getattr(block, "type", None) == "text"
                    )
                    await me.respond(cid, payload={"text": text}, status="ok")
        finally:
            await me.leave()


if __name__ == "__main__":
    asyncio.run(main())
