from __future__ import annotations

import os
from pathlib import Path

import pytest

from codex_thread_console.pty_bridge import PtySession


@pytest.mark.asyncio
async def test_write_all_retries_partial_nonblocking_writes(
    tmp_path, monkeypatch
) -> None:
    session = PtySession(
        "thread",
        tmp_path,
        executable=Path("/tmp/codex"),
        remote_url="ws://127.0.0.1:43123",
    )
    session.master_fd = 42
    writes: list[bytes] = []

    def partial_write(fd: int, data: memoryview) -> int:
        assert fd == 42
        payload = bytes(data)
        writes.append(payload)
        return min(2, len(payload))

    monkeypatch.setattr(os, "write", partial_write)
    await session.write_all("hello")

    assert writes == [b"hello", b"llo", b"o"]
