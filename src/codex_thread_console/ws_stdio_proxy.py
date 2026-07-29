from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

import websockets


_TAP_METHODS = {
    "error",
    "item/completed",
    "item/started",
    "thread/archived",
    "thread/deleted",
    "thread/name/updated",
    "thread/status/changed",
    "thread/unarchived",
    "turn/completed",
    "turn/started",
    "warning",
}
_TAP_SCALAR_KEYS = {
    "error",
    "message",
    "reason",
    "status",
    "threadId",
    "turnId",
    "warning",
}
_TAP_NESTED_KEYS = {
    "item": {"command", "id", "name", "status", "type"},
    "turn": {"error", "id", "status"},
}
_MAX_TAP_TEXT = 2048


def _compact_value(value: Any) -> Any:
    if isinstance(value, str):
        return value[:_MAX_TAP_TEXT]
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, list):
        return [_compact_value(item) for item in value[:32]]
    if isinstance(value, dict):
        return {
            str(key): _compact_value(item)
            for key, item in list(value.items())[:32]
        }
    return str(value)[:_MAX_TAP_TEXT]


def compact_notification(message: str) -> bytes | None:
    """Return a bounded lifecycle notification for the side-channel tap."""
    try:
        payload = json.loads(message)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    method = payload.get("method")
    params = payload.get("params")
    if method not in _TAP_METHODS or not isinstance(params, dict):
        return None

    compact: dict[str, Any] = {}
    for key in _TAP_SCALAR_KEYS:
        if key in params:
            compact[key] = _compact_value(params[key])
    for key, allowed in _TAP_NESTED_KEYS.items():
        value = params.get(key)
        if isinstance(value, dict):
            compact[key] = {
                nested_key: _compact_value(value[nested_key])
                for nested_key in allowed
                if nested_key in value
            }
    return (
        json.dumps(
            {"method": method, "params": compact},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        + b"\n"
    )


async def _stdin_to_websocket(websocket: websockets.ClientConnection) -> None:
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    loop = asyncio.get_running_loop()
    await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    while line := await reader.readline():
        await websocket.send(line.decode("utf-8").rstrip("\n"))


async def _websocket_to_stdout(
    websocket: websockets.ClientConnection,
    tap_writer: asyncio.StreamWriter | None = None,
) -> None:
    async for message in websocket:
        if isinstance(message, bytes):
            payload = message
            text = message.decode("utf-8", errors="replace")
        else:
            payload = message.encode("utf-8")
            text = message
        sys.stdout.buffer.write(payload + b"\n")
        sys.stdout.buffer.flush()
        if tap_writer is not None:
            compact = compact_notification(text)
            if compact is not None:
                try:
                    tap_writer.write(compact)
                    await tap_writer.drain()
                except (ConnectionError, OSError):
                    tap_writer.close()
                    tap_writer = None


async def _connect_tap(port: int | None) -> asyncio.StreamWriter | None:
    token = os.environ.get("CODEX_CONSOLE_TAP_TOKEN")
    if port is None or not token:
        return None
    try:
        _reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(token.encode("utf-8") + b"\n")
        await writer.drain()
        return writer
    except OSError:
        return None


async def run(endpoint: str, tap_port: int | None = None) -> None:
    tap_writer = await _connect_tap(tap_port)
    try:
        async with websockets.connect(endpoint, max_size=None) as websocket:
            tasks = {
                asyncio.create_task(_stdin_to_websocket(websocket)),
                asyncio.create_task(
                    _websocket_to_stdout(websocket, tap_writer)
                ),
            }
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            results = await asyncio.gather(
                *done, *pending, return_exceptions=True
            )
            for result in results:
                if isinstance(result, Exception) and not isinstance(
                    result, asyncio.CancelledError
                ):
                    raise result
    finally:
        if tap_writer is not None:
            tap_writer.close()
            await asyncio.gather(
                tap_writer.wait_closed(),
                return_exceptions=True,
            )


def main() -> None:
    if len(sys.argv) not in {2, 3}:
        raise SystemExit(
            "usage: python -m codex_thread_console.ws_stdio_proxy URL [TAP_PORT]"
        )
    asyncio.run(run(sys.argv[1], int(sys.argv[2]) if len(sys.argv) == 3 else None))


if __name__ == "__main__":
    main()
