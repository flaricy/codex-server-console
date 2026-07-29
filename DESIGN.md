# Codex Thread Console — shared app-server design

## 1. Goal

Provide one local control plane for Codex threads:

- the shell controller starts, steers, queues, and interrupts turns through the
  official Python SDK;
- the Web UI renders the real Codex TUI and can also send, steer, queue, and
  interrupt;
- a turn started through the SDK appears live in the Web TUI;
- mutations received by the backend execute FIFO per thread;
- the Python controller uses `openai-codex==0.144.4`, while the app-server and
  remote TUI use the user's current Codex installation.

This is a trusted, loopback-only experiment, not a multi-user service.

## 2. Runtime compatibility

The Codex CLI exposes all primitives required for the shared topology:

```text
codex app-server --listen ws://127.0.0.1:PORT
codex --remote ws://127.0.0.1:PORT -C DIR resume THREAD
```

The pinned Python SDK supports `CodexConfig.launch_args_override`. It normally launches
its own `codex app-server --listen stdio://`; this project instead launches a tiny
stdio↔WebSocket proxy that connects the SDK to the backend-owned app-server.

The app-server must not be older than the Codex version that last migrated
`~/.codex/state_*.sqlite`. In the verified environment, Codex 0.146 had applied
the `drop agent jobs` migration; an older 0.144.4 app-server then failed
`thread/delete` because it still queried that removed table. Therefore the runtime
prefers `CODEX_CONSOLE_CODEX_BIN` or the installed `codex` and uses the
SDK-bundled CLI only as a fallback. Health output exposes both SDK and app-server
versions.

The following was verified end to end with the real CLI:

1. start one loopback app-server;
2. attach a remote TUI to a thread;
3. start a turn through the shell controller and SDK;
4. observe the prompt and `Working` state in the already attached TUI;
5. steer and interrupt through the Web API;
6. observe the interruption in that same TUI.

## 3. Architecture

```text
                                      one process / one thread authority
                                ┌──────────────────────────────────────┐
Shell controller ── HTTP ──────>│ FastAPI                             │
                                │  ├─ typed command/API layer          │
Web composer ───── HTTP ───────>│  ├─ per-thread MutationSequencer    │
                                │  ├─ ThreadManager + SQLite queue     │
                                │  └─ Python SDK                       │
                                │       │ stdio                        │
                                │       v                              │
                                │     WS stdio proxy ───────┐          │
                                └───────────────────────────┼──────────┘
                                                            v
                                                  shared Codex app-server
                                                            ^
                                ┌───────────────────────────┼──────────┐
Browser xterm <─ terminal WS <──│ PTY: codex --remote ... resume ...   │
                                └──────────────────────────────────────┘
```

The backend owns the app-server lifecycle. It binds to a dynamically allocated
loopback port, writes the endpoint to the mode-`0600` runtime directory, and removes
the endpoint when it shuts down.

The browser never receives the app-server endpoint and never speaks its RPC
protocol directly. It receives PTY bytes from a specifically spawned remote Codex
TUI.

## 4. Atomic mutation model

Every backend mutation is submitted to `MutationSequencer`:

- a global monotonic number is assigned at acceptance;
- each thread has an independent FIFO;
- only one mutation for a thread executes at a time;
- different threads execute concurrently;
- responses and events expose `mutation_sequence`.

The serialized operations include:

```text
thread.rename
thread.archive
thread.restore
turn.start
turn.steer
turn.interrupt
turn.queue
queue.cancel
queue.retry
```

This gives deterministic arrival ordering for shell and Web API requests handled by
the backend. A direct keystroke in the TUI becomes an RPC from the remote CLI rather
than an HTTP mutation. Those RPCs and SDK RPCs are finally serialized by the single
app-server, which is the authoritative thread state machine.

The app-server and SQLite cannot participate in one ACID transaction. Therefore a
crash during `turn/start` remains `indeterminate`, never automatically replayed.
“Atomic” here means ordered mutation execution and a single Codex state authority;
it does not claim distributed exactly-once delivery.

## 5. Thread and terminal state

The controller tracks only states it must supervise:

```text
IDLE ── turn.start ──> SDK_TURN ── completed/interrupted ──> IDLE
  ^                         |
  └──── confirmed idle <────┘

RECONCILING ── confirmed idle ──> IDLE
```

A terminal attachment is orthogonal state:

```text
terminal_attached: false | true
terminal_count: 0..N
```

Attaching a remote TUI does not acquire exclusive thread ownership and does not
block an SDK turn. This is the central difference from the discarded design that
ran independent local app-server processes.

`steer` and typed `interrupt` require the live `TurnHandle` held by this backend.
A turn started manually inside the TUI can still be controlled with the TUI's own
keyboard interaction.

## 6. Commands and HTTP surface

Shell commands:

```text
help
thread list [--archived] [--all-local]
thread create [--name NAME] [--cwd PATH]
thread rename THREAD NAME
thread archive|delete THREAD
thread restore THREAD
thread status THREAD
message send THREAD TEXT
message steer THREAD TEXT
message queue THREAD TEXT
message interrupt THREAD
queue list [THREAD]
queue cancel|retry ID
terminal command THREAD
```

Typed endpoints:

```text
GET    /api/health
GET    /api/threads
POST   /api/threads
PATCH  /api/threads/{id}
DELETE /api/threads/{id}
POST   /api/threads/{id}/archive
POST   /api/threads/{id}/restore
GET    /api/threads/{id}/status
POST   /api/threads/{id}/messages/send
POST   /api/threads/{id}/messages/steer
POST   /api/threads/{id}/messages/queue
POST   /api/threads/{id}/interrupt
GET    /api/queue
DELETE /api/queue/{id}
POST   /api/queue/{id}/retry
POST   /api/command
WS     /ws/events
WS     /ws/terminal/{thread_id}
```

The shell parser uses `shlex`; no user command is executed by a system shell.

## 7. Terminal protocol

The terminal WebSocket carries:

- server-to-client binary PTY output;
- client-to-server input:
  `{"type":"input","id":"optional","data":"..."}`;
- resize:
  `{"type":"resize","cols":120,"rows":36}`;
- readiness:
  `{"type":"terminal_ready","thread_id":"..."}`;
- acknowledged writes:
  `{"type":"input_ack","id":"..."}`.

The server sends `terminal_ready` only after the remote TUI process starts. PTY
input uses `write_all`: it handles partial nonblocking writes and acknowledges only
after every byte is written.

Browser-side connection generations prevent rapid thread switching from leaving
two PTYs alive or mixing stale output into the current xterm.

## 8. Web UI behavior

- Selecting a thread attaches `codex --remote ... resume THREAD`.
- The TUI always remains visible while SDK turns run.
- While idle, the composer offers typed SDK `send` and durable `queue`.
- During any app-server active turn, it offers `steer` and `queue`.
- The Interrupt button serializes `turn.interrupt`.
- Direct keyboard interaction with the remote TUI remains available.
- Activity entries include mutation sequence numbers.

## 9. Durable queue

SQLite stores FIFO prompt records. Claims use `BEGIN IMMEDIATE` and a conditional
state transition:

```text
queued -> running -> done | failed | indeterminate
```

On restart, stale `running` records become `indeterminate`. The user must explicitly
retry or cancel them. A failed or completed SDK turn triggers dispatch of the next
eligible row.

## 10. Security and lifecycle

- FastAPI and app-server bind only to loopback.
- one server process is enforced with an advisory runtime lock;
- browser mutations require a SameSite HttpOnly session cookie and same-origin
  validation;
- shell commands require the protected runtime token;
- working directories must resolve under `CODEX_CONSOLE_WORKSPACE_ROOT`;
- the app-server endpoint file, token, and SQLite database are mode `0600`;
- the app-server and SDK proxy are child processes and are terminated on shutdown;
- the last PTY detach interrupts a TUI-originated active turn before process exit;
- server shutdown resolves and interrupts active managed turn IDs before stopping
  app-server, and cleanup failures do not skip later process cleanup stages;
- the browser cannot choose an executable or arbitrary app-server endpoint;
- PTY access is equivalent to a local interactive Codex CLI and is intended only
  for a trusted local user.

## 11. Verification

Automated tests cover:

- command parsing and authorization;
- thread CRUD and queue recovery;
- turn send/steer/interrupt behavior;
- PTY readiness and input acknowledgements;
- shared PTY attachment during an SDK turn;
- FIFO mutation order per thread;
- concurrency across different threads.

The real smoke test additionally verifies:

- shell SDK prompt appears in the attached Web TUI;
- Web SDK prompt appears in the same TUI;
- Web steer is accepted for the active turn;
- Web interrupt appears as `Conversation interrupted`;
- returned mutation sequences increase in arrival order.

## 12. Non-goals

- multi-user hosting or multiple backend replicas;
- distributed transactions between SQLite and Codex;
- exactly-once recovery after process death;
- exact browser-terminal parity for every native terminal extension or shortcut.
