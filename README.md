# sessionweave-client (Python)

Async client for the [SessionWeave relay](https://github.com/ReInventYourSolutions/sessionweave-broker),
covering the relay's REST surface (`/v1/...`) and the WebSocket events
endpoint (`wss://.../events?token=…`). Use it from any Python runtime
that doesn't have an MCP client built in (or where you'd rather not
plumb one) and still want to participate in a SessionWeave session.

For runtimes that *do* speak MCP (Claude Code, opencode, etc.) the
[`sessionweave-participant` skill][skill] in the parent repo is the
preferred path.

[skill]: ../../skills/sessionweave-participant/

## Install

The SDK isn't on PyPI yet (sub-project #1 surface); install editable
straight from the repo for now:

```bash
pip install -e SDK/python
```

Requires Python 3.11+, `httpx>=0.27`, and `websockets>=12.0`.

## Quick start

```python
import asyncio
from sessionweave_client import SessionWeaveClient

async def main() -> None:
    async with SessionWeaveClient() as client:  # default: relay.sessionweave.com
        # Create a session and join it as the host.
        sess = await client.create_session(
            purpose={"kind": "smoke", "instructions": "echo what you receive"}
        )
        host = await client.join(
            sess["session_id"], role="host", capabilities=["broadcast"]
        )

        # In a real app a worker would join from another process.
        worker = await client.join(
            sess["session_id"], role="worker", capabilities=["respond"]
        )

        # Host dispatches a command.
        cmd = await host.send(
            payload={"q": "ping"},
            dispatch="specific",
            target={"participant_ids": [worker.participant_id]},
            timeout_seconds=30,
        )

        # Worker drains its inbox and responds.
        for evt in await worker.drain_inbox():
            if evt.name == "command.received":
                await worker.respond(
                    evt.payload["command_id"], payload={"a": "pong"}, status="ok"
                )

        # Host drains and sees the result + completion.
        for evt in await host.drain_inbox():
            print(evt.name, evt.payload)

        await worker.leave()
        await host.leave()

asyncio.run(main())
```

## Live event stream (WebSocket)

For workers that want push delivery instead of polling
`drain_inbox`, open the WebSocket events endpoint via
`Participant.events()`. The relay drains any pending buffered events
at attach time and forwards them over the socket before the live
stream starts, so a fresh connection doesn't need a separate REST
drain to seed itself.

```python
async with worker.events() as stream:
    async for evt in stream:
        if evt.name == "command.received":
            await worker.respond(
                evt.payload["command_id"], payload={"a": "pong"}, status="ok"
            )
            break  # exit after first command (demo-only)
```

If the socket drops mid-session, anything the relay had handed to the
WS subscriber but couldn't actually deliver is pushed back to the
per-participant buffer. Reconnect via `events()` again *or* call
`drain_inbox()` to recover.

## Dispatch modes

Match the relay's schema exactly:

| `dispatch` | `target`                          | What it does                                  |
|------------|-----------------------------------|-----------------------------------------------|
| `specific` | `{"participant_ids": [...]}`      | Sends to listed participant_ids               |
| `all`      | `{"role": "..."}` or `{"capability": "..."}` (or both) | Sends to every participant matching the filter |
| `any`      | same shape as `all`               | Picks one matching participant (round-robin)  |

`dispatch="all" / "any"` requires at least one of `target.role` or
`target.capability`.

## Error model

All non-2xx relay responses raise a typed exception so callers can
branch on `except` rather than inspecting strings:

| Relay code              | Exception                |
|-------------------------|--------------------------|
| `session_not_found`     | `SessionNotFoundError`   |
| `not_in_session`        | `NotInSessionError`      |
| `already_responded`     | `AlreadyRespondedError`  |
| `validation_error` and friends | `ValidationError` |
| anything else           | `SessionWeaveError`      |

All inherit from `SessionWeaveError`, which carries the original
`code`, HTTP `status`, and human-readable `message`.

## Examples

See [`examples/`](./examples/) for runnable demos including:

- `01_smoke.py` — host + worker round trip in one process (no LLM).
- `02_anthropic_worker.py` — worker driven by the Anthropic Python SDK.
- `03_openai_worker.py` — worker driven by the OpenAI Python SDK.

## Versioning

SemVer. `0.x` while the relay is still in sub-project #1 (anonymous,
transient sessions, bearer-as-participant_id auth). When sub-project
#3 (accounts + JWT) lands, the auth shape changes — the SDK will
absorb that with a minor bump and keep the public surface stable.
