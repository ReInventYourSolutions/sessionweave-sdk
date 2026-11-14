# sessionweave-client (Python)

Async client for the [SessionWeave relay](https://relay.sessionweave.com),
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
        # create_session returns one-time credentials, incl. a join token.
        jt = sess["join_token"]
        host = await client.join(
            sess["session_id"], join_token=jt, role="host", capabilities=["broadcast"]
        )

        # In a real app a worker would join from another process.
        # (Or: ``await client.join_link(sess["invite_link"], role="worker")``.)
        worker = await client.join(
            sess["session_id"], join_token=jt, role="worker", capabilities=["respond"]
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

        await worker.leave(feedback={
            "summary": "echo round trip clean",
            "rating": 5,
            "issues": [],
            "metrics": {"commands_received": 1, "commands_responded": 1},
        })
        await host.leave(feedback={"summary": "demo done", "rating": 5})

asyncio.run(main())
```

## Persistence — opt-in, default Zero Data Retention

Sessions are **ephemeral by default**: the relay holds them only in RAM
and never writes anything to disk. When you want a session to survive
a relay restart, expose its history to late joiners, or be terminated
only by an explicit delete, pass a `persistence` block at create
(relay 0.16.0+ · SDK 0.5.0+):

```python
r = await client.create_session(
    purpose={"project": "Q3 report"},
    persistence={"mode": "ttl", "ttl_seconds": 86400},  # or {"mode": "until_deleted"}
    creator_id="alice@team",  # optional, relay 0.17.0+ — echoed back
)
sid           = r["session_id"]
creator_token = r["creator_token"]   # ONE-TIME — save it
creator_id    = r["creator_id"]      # echoed back (uuid4 if not passed)
state         = r["persistence"]     # {"mode":"ttl","expires_at":"…"}
```

`creator_token` is the proof of ownership: you need it to terminate
the session, update its purpose and mint further credentials. Store it
in process memory / vault; the relay never returns it again. See
"Credentials" below for the full model.

`creator_id` is an opaque display handle the relay echoes back. Pass
your own (a user id, an agent label) and you'll get it back verbatim;
omit it and the relay mints a uuid4. Not persisted on the relay in
v0.17.0 — it's a client-side display aid for "created by X" surfaces.

```python
# Read the timeline of a persisted session WITHOUT bringing it into RAM.
# Requires `persistence` set at create — ephemeral sessions raise
# NoHistoryError.
from sessionweave_client import NoHistoryError
try:
    page = await client.get_events(sid, token=creator_token, limit=200)
    print(page["session_state"])   # "hot" | "cold" | "ended"
    for e in page["events"]:
        ...
except NoHistoryError:
    print("session is ephemeral — no events stored")

# Terminate when done
await client.delete_session(sid, creator_token)
```

When you don't pass `persistence`, the session is gone the moment idle
TTL fires — nothing reaches storage. Persistence is **the user's
decision**, declared per session.

## Credentials

No call authenticates by knowing a session id or a `participant_id`.
`create_session` returns (once each) a `creator_token`, a first
`supervisor_token` (+ `supervisor_token_id`), a `join_token` valid for
15 minutes (multi-use), and an `invite_url` / `invite_link` embedding it.

| Credential | Use |
|---|---|
| `join_token` | `client.join(sid, join_token=...)` or `client.join_link(link)`; also read access (`describe`, `get_events`). Expiry does not affect already-joined participants. |
| `participant_token` | Secret returned by `join`; held by the `Participant` (`me.token`) for every later call and the WebSocket. Distinct from `participant_id`. |
| `supervisor_token` | Oversight: `describe`, `get_events`, firehose, plus `post_order` (note, halt, continue, redirect). |
| `creator_token` | Everything above plus `update_purpose`, `delete_session`, `mint_join_token`, `mint_supervisor_token`, `list_supervisor_tokens`, `revoke_supervisor_token`, `remove_participant`. |
| `SW_ADMIN_TOKEN` | Relay operator token; accepted wherever a creator token is. |

```python
jt = (await client.mint_join_token(sid, creator_token))["join_token"]
me = await client.join(sid, join_token=jt, role="worker")
# or from a link: me = await client.join_link("swvs://host/<sid>?jt=<jt>")

info = await client.describe(sid, token=supervisor_token)
await client.update_purpose(sid, creator_token, {"project": "Q4"})
who = await client.access(sid, supervisor_token)   # {"role": ..., "participant_id": ...}

sup = await client.mint_supervisor_token(sid, creator_token)   # {token_id, supervisor_token}
await client.revoke_supervisor_token(sid, creator_token, sup["token_id"])
```

Supervision. A supervisor, creator or admin token posts orders; the creator
or admin removes an agent; a participant replies in the note's thread:

```python
await client.post_order(sid, supervisor_token, kind="halt", target=pid)
await client.post_order(sid, supervisor_token, kind="redirect", target="all",
                        message="Focus on the API tests")
await client.remove_participant(sid, creator_token, pid)  # {removed, join_tokens_invalidated}
await client.post_order(sid, supervisor_token, kind="undo", target=pid,
                        message="Revert the last change")   # pauses like halt
await me.reply_to_note(note_id, "Understood, switching")   # participant side
await me.reply_to_note(note_id, "Reverted a.txt", outcome={
    "status": "partial", "undone": ["a.txt"], "not_reversible": ["git push"]})
```

Roster entries carry `supervision_state` (`"active"` / `"paused"`). New typed
errors: `ParticipantPausedError` (command to a halted participant),
`NoteNotFoundError`, `ParticipantRemovedError` (you were removed: stop, do not leave).

Wrong or expired tokens raise `PermissionDeniedError` / `NotInSessionError`;
repeated bad tokens raise `SessionWeaveError` with code `rate_limited` (429).

### Idempotent retries via `command_id`

For retry-safe dispatches over flaky networks, generate a stable
`command_id` and reuse it on retry — a second `send` with the same id
returns the original response, no duplicate event:

```python
import uuid
cid = uuid.uuid4().hex
await worker.send(payload={"q": "?"}, command_id=cid)
# transient failure — retry with the SAME id
await worker.send(payload={"q": "?"}, command_id=cid)   # idempotent
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

## Bounded long-poll (`wait_for_event`)

`Participant.wait_for_event()` blocks server-side until a matching event
arrives or the timeout elapses — zero inference cost while waiting. Use it
instead of a busy-poll loop when your worker can't keep a WebSocket open
but still wants event-driven behaviour.

```python
# Bounded long-poll loop — keeps a turn alive without busy-polling
while True:
    result = await me.wait_for_event(
        names=["command.received", "supervisor.note"],
        timeout_seconds=60,
    )
    if result.messages:
        for evt in result.messages:
            await handle(evt)
    elif result.timed_out:
        if should_exit():
            break
```

Non-matching events stay in the buffer and remain visible to subsequent
`drain_inbox` or `wait_for_event` calls. Returns a `WaitForEventResult`
with fields `messages: list[Event]` and `timed_out: bool`.

## Exit feedback (mandatory by skill convention)

`Participant.leave()` accepts an optional `feedback` dict. The relay
propagates it on the broadcast `participant.left` event so the host
collects per-worker exit reports without instrumenting the workers
themselves. Recommended shape (per skill v0.11.0+):

```python
await me.leave(feedback={
    "summary": "echo handled 2 commands cleanly",  # one-line summary
    "rating": 5,                                     # 1..5
    "issues": [],                                    # observed anomalies
    "metrics": {                                     # client-side counters
        "commands_received": 2,
        "commands_responded": 2,
        "wall_seconds": 47,
    },
})
```

The relay enforces a 4 KiB JSON cap on `feedback`. The host can
collect feedback by draining `participant.left` events from its
inbox — `evt.payload["feedback"]` carries the dict the worker passed.

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
| `invalid_delivery_mode` | `InvalidDeliveryModeError` |
| `wait_already_in_flight` | `WaitAlreadyInFlightError` |
| `participant_evicted`   | `ParticipantEvictedError` |
| `too_many_waits`        | `TooManyWaitsError`      |
| anything else           | `SessionWeaveError`      |

All inherit from `SessionWeaveError`, which carries the original
`code`, HTTP `status`, and human-readable `message`.

## Examples

See [`examples/`](./examples/) for runnable demos including:

- `01_smoke.py` — host + worker round trip in one process (no LLM).
- `02_anthropic_worker.py` — worker driven by the Anthropic Python SDK.
- `03_openai_worker.py` — worker driven by the OpenAI Python SDK.

## Versioning

SemVer. `0.x` while the relay is still pre-accounts. Authentication is
token-based: join tokens, secret participant tokens, supervisor and
creator tokens (see "Credentials"); a future accounts/JWT layer will be
absorbed with a minor bump.

| SDK | Relay | What's in it |
|-----|-------|--------------|
| 0.1.0 | 0.7.0 – 0.9.3 | Initial release: `SessionWeaveClient` + `Participant`, REST + WS event stream, typed exceptions |
| 0.2.0 | 0.10.0+        | `Participant.leave(feedback=…)` for the structured exit report |
| 0.3.0 | 0.13.2+        | `Participant.wait_for_event(names=, timeout_seconds=)` long-poll primitive; `WaitForEventResult`; four new typed exceptions |
| 0.4.0 | 0.15.0+        | `Participant.relay_epoch` exposed for cold-start vs restart distinction |
| 0.5.0 | 0.16.0+        | Persistence (`ttl` / `until_deleted`), `creator_token` flow, `get_events`, `session_delete`, `command_id` idempotency |
| 0.6.0 | 0.17.0+        | `create_session(creator_id=…)` echo + firehose creator-token auth |
| 0.7.0 | 0.17.3+        | `Participant.wait_for_event()` now extends the per-call httpx read timeout to `timeout_seconds + 10` so the connection survives the server-side budget — eliminates the spurious `httpx.ReadTimeout` per turn that you'd otherwise paper over with a relay-side cancellation (relay 0.17.3 also added that safety net via HTTP `499 client_disconnect`, so the cleanup is correct either way). |
| 0.8.0 | 0.28.0+        | Join tokens, secret participant tokens, credential minting (`creator_token`, supervisor tokens), token-authenticated reads; `join_link` refuses a mismatched relay host. BREAKING: no session-id or participant_id authentication |
| 0.9.0 | 0.29.0+        | `post_order` (note/halt/continue/redirect) and `remove_participant`; `Participant.reply_to_note`; typed errors for paused and removed participants (`participant_paused`, `participant_removed`) |
| 0.10.0 | 0.30.0+       | `post_order` accepts `undo` (message required); `Participant.reply_to_note(outcome=…)` structured undo outcome |
| 0.10.0 | 0.30.0+       | `post_order` accepts `undo`; `Participant.reply_to_note(..., outcome=…)` (status done/partial/impossible, undone, not_reversible) |
