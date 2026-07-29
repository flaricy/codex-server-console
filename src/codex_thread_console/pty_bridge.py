from __future__ import annotations

import asyncio
import fcntl
import os
import pty
import signal
import struct
import subprocess
import termios
from pathlib import Path

from codex_cli_bin import bundled_codex_path


class PtySession:
    def __init__(
        self,
        thread_id: str,
        cwd: Path,
        *,
        executable: Path | None = None,
        remote_url: str | None = None,
    ):
        self.thread_id = thread_id
        self.cwd = cwd
        self.executable = executable or bundled_codex_path()
        self.remote_url = remote_url
        self.master_fd: int | None = None
        self.process: subprocess.Popen[bytes] | None = None

    def start(self, cols: int = 120, rows: int = 36) -> None:
        master, slave = pty.openpty()
        self.master_fd = master
        self.resize(cols, rows)
        env = dict(os.environ)
        env["TERM"] = "xterm-256color"
        command = [str(self.executable)]
        if self.remote_url:
            command.extend(["--remote", self.remote_url])
        command.extend(["-C", str(self.cwd), "resume", self.thread_id])
        try:
            self.process = subprocess.Popen(
                command,
                stdin=slave,
                stdout=slave,
                stderr=slave,
                cwd=self.cwd,
                env=env,
                start_new_session=True,
                close_fds=True,
            )
        finally:
            os.close(slave)
        os.set_blocking(master, False)

    def resize(self, cols: int, rows: int) -> None:
        if self.master_fd is None:
            return
        cols = min(max(int(cols), 20), 500)
        rows = min(max(int(rows), 5), 200)
        fcntl.ioctl(
            self.master_fd,
            termios.TIOCSWINSZ,
            struct.pack("HHHH", rows, cols, 0, 0),
        )

    async def write_all(self, data: str) -> None:
        if self.master_fd is None:
            raise RuntimeError("PTY is not running")
        remaining = memoryview(data.encode("utf-8"))
        while remaining:
            try:
                written = os.write(self.master_fd, remaining)
            except BlockingIOError:
                await self._wait_writable(self.master_fd)
                continue
            if written <= 0:
                raise OSError("PTY write returned no progress")
            remaining = remaining[written:]

    async def _wait_writable(self, fd: int) -> None:
        loop = asyncio.get_running_loop()
        ready: asyncio.Future[None] = loop.create_future()

        def mark_ready() -> None:
            if not ready.done():
                ready.set_result(None)

        loop.add_writer(fd, mark_ready)
        try:
            await ready
        finally:
            loop.remove_writer(fd)

    async def read(self) -> bytes:
        if self.master_fd is None:
            return b""
        loop = asyncio.get_running_loop()
        future: asyncio.Future[bytes] = loop.create_future()
        fd = self.master_fd

        def ready() -> None:
            if future.done():
                return
            try:
                data = os.read(fd, 65536)
                future.set_result(data)
            except BlockingIOError:
                return
            except OSError:
                future.set_result(b"")
            finally:
                if future.done():
                    loop.remove_reader(fd)

        loop.add_reader(fd, ready)
        try:
            return await future
        finally:
            loop.remove_reader(fd)

    async def stop(self) -> None:
        process = self.process
        if process is None:
            self._close_fd()
            return
        if process.poll() is None:
            for sig, timeout in (
                (signal.SIGHUP, 1.5),
                (signal.SIGTERM, 1.5),
                (signal.SIGKILL, 1.0),
            ):
                try:
                    os.killpg(process.pid, sig)
                except ProcessLookupError:
                    break
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(process.wait), timeout=timeout
                    )
                    break
                except asyncio.TimeoutError:
                    continue
        if process.poll() is None:
            await asyncio.to_thread(process.wait)
        self._close_fd()

    def _close_fd(self) -> None:
        if self.master_fd is not None:
            try:
                os.close(self.master_fd)
            except OSError:
                pass
            self.master_fd = None
