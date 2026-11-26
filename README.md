# SessionWeave Python SDK (`sessionweave-sdk`)

> **Guide:** how sessions, roles, invite links, accounts and the agent registry work is in the
> [SessionWeave broker guide](https://github.com/ReInventYourSolutions/sessionweave-broker/blob/main/docs/guide/README.md).

Async client for the [SessionWeave broker](https://github.com/ReInventYourSolutions/sessionweave-broker)
(hosted at <https://broker.sessionweave.com>), covering the broker's REST surface (`/v1/...`)
and the WebSocket events endpoint (`wss://.../events?token=…`). Use it from any Python runtime
that doesn't have an MCP client built in (or where you'd rather not plumb one) and still want
to participate in a SessionWeave session.

For runtimes that *do* speak MCP (Claude Code, opencode, etc.) the
[`sessionweave-participant` skill][skill] is the preferred path.

[skill]: https://github.com/ReInventYourSolutions/sessionweave-broker/tree/main/skills/sessionweave-participant

## Install

The distribution is called `sessionweave-sdk`; the import package is `sessionweave_client`:

```bash
pip install sessionweave-sdk
```

```python
import sessionweave_client
```

Requires Python 3.11+, `httpx>=0.27`, and `websockets>=12.0`.

## Quick start

```python
import asyncio
from sessionweave_client import SessionWeaveClient

async def main() -> None:
    async with SessionWeaveClient() as client:  # default: broker.sessionweave.com
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

## Accounts

An account owns sessions and holds account tokens. Pass one as
`SessionWeaveClient(account_token=...)` or set `SW_ACCOUNT_TOKEN` (pass `account_token=""` to ignore the
environment variable); it is
sent only by `create_session` (the session becomes the account's) and the
`me*` methods. Joining never needs it, and it is never sent on join,
participant or management calls.

```python
async with SessionWeaveClient(account_token="...") as client:
    session = await client.create_session(purpose={"kind": "demo"})
    await client.me()                       # the account
    await client.my_tokens()                # {"tokens": [...]}
    t = await client.create_my_token("ci", expires_at="2027-01-01T00:00:00Z")
    await client.revoke_my_token(t["token_id"])
    await client.my_sessions()              # {"sessions": [...]}
```

Without an account token the `me*` methods raise `ValueError` before any
request. Relay operators administer accounts with the admin token passed
explicitly: `admin_create_account`, `admin_list_accounts`,
`admin_get_account`, `admin_update_account`, `admin_delete_account`,
`admin_create_account_token`, `admin_revoke_account_token`; and
`create_session(owner_account_id=..., admin_token=...)` creates a session for an account (the admin token is then the Bearer; `owner_account_id` without it raises `ValueError`).
All return the parsed JSON (`None` for revocations) and raise the usual
typed errors.

### Token networks, approvals and account events

Account tokens can be bound to networks. `create_my_token(label, network_policy=..., approved_networks=[...])`
(and `admin_create_account_token`) set the binding at creation;
`my_token_networks(token_id)` / `set_my_token_networks(token_id, network_policy=..., approved_networks=...)`
read and change it. A call from an unapproved network fails with code
`network_not_approved` (HTTP 403) and creates a pending approval:
`my_network_approvals()`, `approve_my_network(approval_id, scope="ip"|"network")`,
`deny_my_network(approval_id)` (unknown id: `approval_not_found`, 404). Admin
equivalents: `admin_token_networks`, `admin_set_token_networks`,
`admin_network_approvals`, `admin_approve_network`, `admin_deny_network`
(all take `admin_token`, `account_id`).

```python
page = await client.my_events(after=0, wait=30)   # {"events", "next", "truncated"}
page = await client.my_events(after=page["next"], wait=30)
await client.admin_account_events(admin_token, after=0, account_id="acc_1", wait=30)
```

`wait` long-polls (max 60 s); the HTTP read timeout is extended to `wait + 10` s.
The deny calls return `None` (the relay answers 204); the others return the parsed JSON.
Errors surface as `SessionWeaveError` with `code`, `message` and `status`.

## Machines and agent definitions

Launchers started with `--local <folder>` or `--root` accept agent definitions
deployed through the broker. In account mode only the launcher owner's account
token may deploy, browse and list processes (`/v1/me/launchers...`):

```python
machines = await client.my_launchers()                      # {"launchers": [...]}
key = machines["launchers"][0]["launcher_key"]
await client.deploy_my_definition(key, {"id": "web-obs", "domain": "ops", "role": "observer",
    "runtime": "claude", "kind": "observer", "workdir": "/srv/app"})  # {"definition_id", "revision", "state"}
page = await client.browse_my_launcher(key, "/srv", limit=50, hidden=False)
procs = await client.my_launcher_processes(key, query="nginx")
await client.remove_my_definition(key, "web-obs")
await client.forget_my_launcher(key)                        # offline machines only; returns None
```

On an open-mode broker (LAN-only trust, needs the broker's persistence DB) the
same calls without the `my_` prefix (`launchers`, `launcher`, `deploy_definition`,
`remove_definition`, `forget_launcher`, `browse_launcher`, `launcher_processes`)
use `/v1/launchers...` and never send a credential, even when an account token
is configured. Observers on a `--root` machine may set `observer_as_root: true`
in the definition. Relay errors surface as `SessionWeaveError` with `code`:
`launcher_offline` (409), `launcher_timeout` (504), `launcher_bad_reply` (502),
`path_not_found` (404), `definition_in_use` / `definition_limit` (409),
`definitions_unavailable` (503), `launcher_not_found` (404).

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
# The session's coordinator may remove the agents it launched with its own
# participant token: client.remove_participant(sid, coordinator_token, agent_pid)
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

See [`examples/`](https://github.com/ReInventYourSolutions/sessionweave-sdk/tree/main/examples) for runnable demos including:

- `01_smoke.py` — host + worker round trip in one process (no LLM).
- `02_anthropic_worker.py` — worker driven by the Anthropic Python SDK.
- `03_openai_worker.py` — worker driven by the OpenAI Python SDK.

## Versioning

SemVer. `0.x` while the API is still settling. Authentication is
token-based: join tokens, secret participant tokens, supervisor and
creator tokens (see "Credentials"). Broker accounts add an optional
account token for creating and managing sessions; it does not change the
participant surface.

| SDK | Broker | What's in it |
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
| 0.10.0 | 0.30.0+       | `post_order` accepts `undo` (message required); `Participant.reply_to_note(..., outcome=…)` structured undo outcome (status done/partial/impossible, undone, not_reversible) |
| 0.11.0 | 0.31.0+       | Accounts: `account_token` / `SW_ACCOUNT_TOKEN`, `create_session(owner_account_id=…)`, `me`, `my_tokens`, `create_my_token`, `revoke_my_token`, `my_sessions`, and `admin_*` account administration |
| 0.11.1 | 0.31.0+       | `SessionWeaveClient(transport=…, ws_connect=…)`: inject the httpx transport and the WebSocket connector (address-pinning embedders such as the Cockpit) |
| 0.12.0 | 0.33.0+       | Token networks (`network_policy`, `approved_networks`), network approvals, account events (`my_events`, `admin_account_events`) |
| 0.13.0 | 0.34.0+       | Agent launchers: `Participant.discover_agents`, `list_domains`, `launch_agent`, `launch_status` (the session coordinator; the REST routes also take the creator/admin credential), `SessionWeaveClient.set_coordinator` |
| 0.14.0 | 0.36.0+       | Machines and agent definitions: `my_launchers`, `my_launcher`, `deploy_my_definition`, `remove_my_definition`, `forget_my_launcher`, `browse_my_launcher`, `my_launcher_processes` (account) and the open-mode twins `launchers`, `launcher`, `deploy_definition`, `remove_definition`, `forget_launcher`, `browse_launcher`, `launcher_processes`; every id placed in a URL path (launcher, account, token, approval, note, launch, session ids) that is empty, only dots (`.`, `..`) or contains `/` raises `ValueError` before any request |

## License

Apache License 2.0 — see [`LICENSE`](https://github.com/ReInventYourSolutions/sessionweave-sdk/blob/main/LICENSE)
and [`NOTICE`](https://github.com/ReInventYourSolutions/sessionweave-sdk/blob/main/NOTICE).
