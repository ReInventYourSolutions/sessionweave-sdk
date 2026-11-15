# Examples

| File | What it shows | Required deps |
|------|---------------|---------------|
| `01_smoke.py` | Host + worker round trip in one process. No LLM provider — useful as a connectivity / contract check. | `sessionweave-client` |
| `02_anthropic_worker.py` | A worker that joins an existing session, listens on the WS event stream, and answers `command.received{prompt: "..."}` events using the Anthropic SDK. | `sessionweave-client`, `anthropic` |
| `03_openai_worker.py` | Same shape as (2) but using OpenAI's `chat.completions.create`. | `sessionweave-client`, `openai` |

All examples accept `SW_BASE` to point at a different relay (defaults
to `https://broker.sessionweave.com`). The Anthropic / OpenAI workers
expect `SW_SESSION_ID` for the session to join, and the usual provider
API key env var. Specific model overrides via `ANTHROPIC_MODEL` /
`OPENAI_MODEL`.

Recipe for a quick two-vendor demo:

```bash
# 1. Create a session anywhere — even with curl:
SID=$(curl -s -X POST https://broker.sessionweave.com/v1/sessions \
  -H 'content-type: application/json' \
  -d '{"purpose":{"kind":"two-vendor-demo"}}' | jq -r .session_id)

# 2. Boot one worker per vendor in two terminals:
SW_SESSION_ID=$SID python 02_anthropic_worker.py
SW_SESSION_ID=$SID python 03_openai_worker.py

# 3. From a third terminal, drive the session as a host:
python -c "
import asyncio, os
from sessionweave_client import SessionWeaveClient
async def main():
    async with SessionWeaveClient() as c:
        host = await c.join(
            os.environ['SW_SESSION_ID'], join_token=os.environ['SW_JOIN_TOKEN'], role='host',
        )
        cmd = await host.send(
            payload={'prompt': 'Explain SessionWeave in one sentence.'},
            dispatch='all',
            target={'capability': 'respond'},
            timeout_seconds=120,
        )
        print('cmd', cmd['command_id'], '→', cmd['delivered_to'])
        for _ in range(60):
            for evt in await host.drain_inbox():
                print(evt.name, evt.payload)
            await asyncio.sleep(1)
        await host.leave()
asyncio.run(main())
" SW_SESSION_ID=$SID
```

The host's drain shows two `result.received` events (one per vendor)
followed by a `command.completed` with `received_count: 2`.
