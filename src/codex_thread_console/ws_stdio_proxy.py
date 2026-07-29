from __future__ import annotations

import asyncio
import sys

import websockets


async def _stdin_to_websocket(websocket: websockets.ClientConnection) -> None:
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    loop = asyncio.get_running_loop()
    await loop.connect_read_pipe(lambda: protocol, sys.stdin.buffer)
    while line := await reader.readline():
        await websocket.send(line.decode("utf-8").rstrip("\n"))


async def _websocket_to_stdout(websocket: websockets.ClientConnection) -> None:
    async for message in websocket:
        if isinstance(message, bytes):
            payload = message
        else:
            payload = message.encode("utf-8")
        sys.stdout.buffer.write(payload + b"\n")
        sys.stdout.buffer.flush()


async def run(endpoint: str) -> None:
    async with websockets.connect(endpoint, max_size=None) as websocket:
        tasks = {
            asyncio.create_task(_stdin_to_websocket(websocket)),
            asyncio.create_task(_websocket_to_stdout(websocket)),
        }
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()
        results = await asyncio.gather(*done, *pending, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception) and not isinstance(
                result, asyncio.CancelledError
            ):
                raise result


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit("usage: python -m codex_thread_console.ws_stdio_proxy URL")
    asyncio.run(run(sys.argv[1]))


if __name__ == "__main__":
    main()
