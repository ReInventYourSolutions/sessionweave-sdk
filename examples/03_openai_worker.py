"""A SessionWeave worker driven by the OpenAI Python SDK.

Mirror of ``02_anthropic_worker.py`` but using OpenAI's
``chat.completions.create``. Demonstrates that SessionWeave is
genuinely vendor-neutral — any LLM runtime that can read text in and
emit text out can participate.

Run:

    pip install openai sessionweave-client
    export OPENAI_API_KEY=...
    export SW_SESSION_ID=<sid>
    export SW_BASE=https://relay.sessionweave.com  # optional override
    python 03_openai_worker.py
"""
from __future__ import annotations

import asyncio
import os

from sessionweave_client import SessionWeaveClient


async def main() -> None:
    try:
        from openai import AsyncOpenAI
    except ImportError as exc:  # pragma: no cover - optional dep
        raise SystemExit("this example requires `pip install openai`") from exc

    sid = os.environ["SW_SESSION_ID"]
    base = os.environ.get("SW_BASE", "https://relay.sessionweave.com")
    model = os.environ.get("OPENAI_MODEL", "gpt-4o")

    openai = AsyncOpenAI()

    async with SessionWeaveClient(base_url=base) as client:
        me = await client.join(
            sid,
            role="openai-worker",
            objective=f"answer prompts using {model}",
            capabilities=["respond", "llm:openai"],
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
                    completion = await openai.chat.completions.create(
                        model=model,
                        messages=[{"role": "user", "content": prompt}],
                    )
                    text = completion.choices[0].message.content or ""
                    await me.respond(cid, payload={"text": text}, status="ok")
        finally:
            await me.leave()


if __name__ == "__main__":
    asyncio.run(main())
