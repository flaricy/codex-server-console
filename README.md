# Codex Thread Console

A local visual debugger and workflow foundation built on the official Codex
Python SDK. It combines:

- a FastAPI control server;
- a durable application-owned FIFO message queue;
- a shell-like HTTP client;
- a browser UI with the actual Codex TUI rendered through xterm.js and a PTY.

Read [DESIGN.md](./DESIGN.md) for the state machine, crash semantics, security
boundary, and the independent design critique that shaped the implementation.

## Install

Requirements: macOS or Linux, Python 3.10+, Node.js, and an existing Codex login.
The Python SDK automatically reuses the local Codex authentication.

```bash
git clone https://github.com/flaricy/codex-server-console.git
cd codex-server-console
./scripts/setup
```

The official SDK includes its matching Codex runtime, which is why a fresh virtual
environment is much larger than this repository. `.venv`, `node_modules`, runtime
data, and built assets are local install artifacts and are never committed.

Resolution order is:

1. `CODEX_CONSOLE_CODEX_BIN`, when explicitly set;
2. `codex` from `PATH`;
3. the runtime bundled with the official Python SDK.

The private runtime inside ChatGPT.app is never used. Even an explicit path or
`PATH` symlink resolving inside the app bundle is rejected. `GET /api/health`
reports the selected public binary, source, and version.

## Run

Choose the directory tree that new threads may use, then start one server process:

```bash
./scripts/run /absolute/path/to/your/workspace
```

Open [http://127.0.0.1:8765](http://127.0.0.1:8765).

The installed command is also available directly:

```bash
.venv/bin/codex-thread-console-server --workspace /absolute/path/to/workspace
```

Only loopback binding and one server worker are supported. Runtime state is stored
under this experiment's `.data/` directory and ignored by Git. Override it with
`CODEX_CONSOLE_DATA_DIR` when needed.

The server starts one child `codex app-server` on a dynamic loopback endpoint.
Both the Python SDK proxy and every Web TUI connect to that same process.

## Architecture

There is exactly one long-running `codex app-server`. It is the authoritative
owner of Codex threads and turns:

```text
Shell REPL ── persistent HTTP ─┐
                              │
Web controls ───── HTTP ──────┼──> FastAPI
                              │      ├─ per-thread FIFO mutation sequencer
                              │      ├─ SQLite durable prompt queue
                              │      └─ Python SDK
                              │             │ stdio
                              │             v
                              │        stdio↔WebSocket proxy ──┐
                              │                                │
Browser xterm <─ WebSocket <──┴── PTY: codex --remote ... ────┤
                                                               v
                                                    one Codex app-server
```

The two paths serve different purposes:

- typed control operations (`send`, `steer`, `queue`, `interrupt`, and thread
  management) go through FastAPI, a FIFO sequencer, and the official Python SDK;
- the browser terminal is the real Codex CLI. FastAPI starts it in a PTY with
  `codex --remote <shared-endpoint> resume <thread>` and forwards raw terminal
  bytes to xterm.js.

The SDK normally starts its own stdio app-server. Here, a small
stdio↔WebSocket proxy redirects it to the same backend-owned app-server used by
every remote TUI. This is what lets a prompt sent from the shell or Web controls
appear in the attached Codex TUI.

Codex app-server state is authoritative. SQLite stores only application-owned
queue and console metadata. FastAPI serializes mutations FIFO per thread, while
different threads may run concurrently. Browser-to-PTY traffic remains genuine
interactive CLI traffic and meets SDK traffic at the shared app-server.

## Shared app-server demo

Keep the server and browser open. In another terminal:

```bash
.venv/bin/codex-thread-console thread create --name shared-demo
.venv/bin/codex-thread-console message send shared-demo \
  "Run sleep 30, then summarize what happened."
```

Select `shared-demo` in the browser. The SDK prompt and live turn state appear in
the real Codex TUI. While it is running, use the Web composer to steer it or click
**Interrupt**. The Activity panel and shell responses include
`mutation_sequence`, showing the per-thread commit order.

Two concurrent `message send` calls are deterministic: the first starts and the
second is stored in the durable FIFO queue. An earlier queued or retried message
also cannot be overtaken by a later `send`.

## Shell client

Run one command:

```bash
.venv/bin/codex-thread-console thread list
.venv/bin/codex-thread-console thread list --created-here
.venv/bin/codex-thread-console thread create --name "demo"
.venv/bin/codex-thread-console thread archive demo
.venv/bin/codex-thread-console message queue demo "Inspect the repository"
.venv/bin/codex-thread-console message steer demo "Focus on correctness"
.venv/bin/codex-thread-console message interrupt demo
.venv/bin/codex-thread-console thread delete demo
```

Or enter the interactive REPL:

```bash
.venv/bin/codex-thread-console
```

The client opens and authenticates a TCP connection before showing the prompt.
If the server is unavailable it exits with a clear error instead of entering a
disconnected shell. The REPL supports terminal line editing, including arrow
keys, Home/End, deletion, and command history for the current process.
Thread and queue results use compact tables by default. Add `--json` before the
command for stable machine-readable output.

The client reads the short-lived server token from the protected runtime data
directory. Set the same `CODEX_CONSOLE_WORKSPACE_ROOT` in the server and client
shells.

## Python workflows

Automation must connect to the running console rather than construct another SDK
client and accidentally launch a second app-server. The included async client is
a thin typed facade over the same control plane used by Web and the shell:

```python
import asyncio

from codex_thread_console import AsyncConsoleClient


async def main() -> None:
    async with AsyncConsoleClient() as console:
        thread = await console.create_thread(
            cwd="/absolute/path/to/workspace",
            name="workflow-demo",
        )
        workflow = console.thread(thread["id"])
        outcome = await workflow.send_and_wait(
            "Inspect the repository and summarize the highest-risk module.",
            options={
                "effort": "high",
                "output_schema": {
                    "type": "object",
                    "properties": {
                        "module": {"type": "string"},
                        "risk": {"type": "string"},
                    },
                    "required": ["module", "risk"],
                },
            },
        )
        print(outcome.json())


asyncio.run(main())
```

`snapshot()` returns the current thread view plus an `event_id`.
`events(thread_id=..., since_event_id=...)` exposes the normalized lifecycle
stream and replays events from that cursor after a disconnect. If the bounded
replay window was exceeded, it yields `resync_required` so the workflow can take
a new snapshot instead of silently operating on stale state.

`send_and_wait` subscribes before starting the turn, follows a queued message into
its eventual turn, and returns its final response. If event loss makes that final
response unknowable, it raises `EventStreamGapError` after the thread is confirmed
idle rather than returning a false success. This keeps the official Python SDK
and app-server authority inside one backend while allowing independent Python
workflow processes.

The thread-bound facade accepts the stable official-SDK overrides `cwd`, `effort`,
`model`, `output_schema`, `personality`, `service_tier`, and `summary`. They are
persisted with queued messages, so a delayed FIFO dispatch has the same semantics
as an immediate turn. Per-turn `cwd` is still constrained to the configured
workspace. Approval and sandbox policy intentionally remain fixed at
`deny_all`/`read_only`; automation cannot use an option to bypass that boundary.

This package deliberately does not recreate the SDK's entire generated protocol.
The current SDK contains generated dynamic-tool wire types but no stable high-level
registration/handler API, so this console does not bind itself to those private
internals. A future dynamic-tool layer should land behind the adapter when the
official SDK exposes that contract.

## Important semantics

- `thread archive` is reversible. `thread delete` calls app-server
  `thread/delete` and permanently removes the session and its local queue state.
- The default list is the authoritative thread domain of the shared app-server.
  “Created here” is optional metadata and a filter, not a different session type.
- The backend starts one loopback `codex app-server` process. The Python SDK connects
  through a stdio↔WebSocket proxy; every browser TUI uses
  `codex --remote <same-endpoint>`. No component starts a second app-server.
- The debugger loads a REST snapshot before consuming ordered WebSocket deltas.
  Each event has a monotonic `event_id`; reconnects replay a bounded in-memory
  window and explicitly request a new snapshot if continuity cannot be proven.
- The right-hand inspector separates structured Activity events from the selected
  thread's durable queue. Queued items expose their persisted turn options;
  `indeterminate` items can be retried or cancelled without using the raw command
  endpoint.
- Selecting a thread is read-only and does not launch a process. **Attach CLI**
  explicitly launches its real remote Codex TUI. A turn started by the shell
  controller or Web composer is rendered live after attachment.
- The Web composer uses typed operations through the backend: `send` while idle,
  `steer` during any active turn, and durable `queue`. The controller resolves the
  authoritative active `turnId`, so steer/interrupt also work for turns started by
  the TUI rather than this server.
- Detaching the PTY only disconnects the local display; the authoritative turn
  keeps running and can be reattached later. Explicit **Interrupt** and Ctrl+C
  server shutdown still interrupt active turns before stopping app-server.
- The browser waits for an explicit `terminal_ready` frame before accepting composer
  input. PTY input with an id is acknowledged only after its complete byte sequence
  is written.
- Backend mutations are assigned a monotonically increasing `mutation_sequence` and
  executed FIFO per thread. Different threads can progress concurrently. Direct TUI
  requests and SDK requests meet at the single app-server, which is the final
  thread-state authority.
- Queue ordering is FIFO during normal operation. SQLite and Codex cannot share an
  atomic transaction, so a crash during `turn/start` produces `indeterminate`, never
  an automatic replay. Use `queue retry ID` or `queue cancel ID` explicitly.
- Background SDK turns use `deny_all` approvals and a read-only sandbox. Interactive
  approvals appear only inside the attached Codex TUI.
- The loopback app-server endpoint is created at startup, recorded in the protected
  runtime directory, and removed when the console shuts down.

## Test

```bash
.venv/bin/python -m pytest -q
npm audit --omit=dev --prefix frontend
npm run build --prefix frontend
```

The automated suite uses a fake Codex adapter and does not consume model usage. The
manual live smoke test is described in [DESIGN.md](./DESIGN.md).
