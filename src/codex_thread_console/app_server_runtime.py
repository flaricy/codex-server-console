from __future__ import annotations

import asyncio
import os
import signal
import shutil
import socket
import sys
from collections import deque
from pathlib import Path

from codex_cli_bin import bundled_codex_path
from openai_codex import CodexConfig


def installed_codex_path() -> tuple[Path, str]:
    """Prefer the Codex installation that owns the user's global state DB."""
    path_codex = shutil.which("codex")
    if path_codex:
        return Path(path_codex).resolve(), "path"
    desktop_codex = Path("/Applications/ChatGPT.app/Contents/Resources/codex")
    if desktop_codex.is_file():
        return desktop_codex, "chatgpt-app"
    return bundled_codex_path(), "sdk-bundled-fallback"


class AppServerRuntime:
    """Own one loopback app-server shared by SDK proxies and remote TUIs."""

    def __init__(
        self,
        runtime_dir: Path,
        *,
        codex_bin: Path | None = None,
        startup_timeout: float = 10.0,
    ) -> None:
        self.runtime_dir = runtime_dir
        self.endpoint_path = runtime_dir / "app-server-endpoint"
        if codex_bin is None:
            self.codex_bin, self.codex_bin_source = installed_codex_path()
        else:
            self.codex_bin = codex_bin
            self.codex_bin_source = "explicit"
        self.startup_timeout = startup_timeout
        self.codex_version = "unknown"
        self.port: int | None = None
        self.process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_tail: deque[str] = deque(maxlen=100)

    @property
    def remote_url(self) -> str:
        if self.port is None:
            raise RuntimeError("app-server has not started")
        return f"ws://127.0.0.1:{self.port}"

    @property
    def proxy_args(self) -> tuple[str, ...]:
        return (
            sys.executable,
            "-m",
            "codex_thread_console.ws_stdio_proxy",
            self.remote_url,
        )

    def sdk_config(self) -> CodexConfig:
        return CodexConfig(launch_args_override=self.proxy_args)

    async def start(self) -> None:
        if self.process is not None:
            return
        if not self.codex_bin.is_file() or not os.access(self.codex_bin, os.X_OK):
            raise RuntimeError(
                f"Codex binary is not executable: {self.codex_bin}"
            )
        self.codex_version = await self._read_codex_version()
        self.runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.port = self._allocate_loopback_port()
        self.process = await asyncio.create_subprocess_exec(
            str(self.codex_bin),
            "app-server",
            "--listen",
            self.remote_url,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        self._stderr_task = asyncio.create_task(self._drain_stderr())
        deadline = asyncio.get_running_loop().time() + self.startup_timeout
        while asyncio.get_running_loop().time() < deadline:
            if self.process.returncode is not None:
                raise RuntimeError(self._startup_error("app-server exited"))
            try:
                reader, writer = await asyncio.open_connection(
                    "127.0.0.1", self.port
                )
            except OSError:
                await asyncio.sleep(0.05)
                continue
            writer.close()
            await writer.wait_closed()
            del reader
            self.endpoint_path.write_text(self.remote_url, encoding="utf-8")
            self.endpoint_path.chmod(0o600)
            return
        await self.close()
        raise RuntimeError(self._startup_error("app-server endpoint did not open"))

    async def _read_codex_version(self) -> str:
        process = await asyncio.create_subprocess_exec(
            str(self.codex_bin),
            "--version",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=5
            )
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            raise RuntimeError("timed out reading Codex version")
        if process.returncode != 0:
            detail = stderr.decode("utf-8", errors="replace").strip()
            raise RuntimeError(
                f"failed to read Codex version"
                f"{f': {detail}' if detail else ''}"
            )
        return stdout.decode("utf-8", errors="replace").strip()

    async def close(self) -> None:
        process = self.process
        self.process = None
        if process is not None and process.returncode is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except asyncio.TimeoutError:
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                await process.wait()
        if self._stderr_task is not None:
            await asyncio.gather(self._stderr_task, return_exceptions=True)
            self._stderr_task = None
        try:
            self.endpoint_path.unlink()
        except FileNotFoundError:
            pass
        self.port = None

    async def _drain_stderr(self) -> None:
        process = self.process
        if process is None or process.stderr is None:
            return
        while line := await process.stderr.readline():
            self._stderr_tail.append(line.decode("utf-8", errors="replace").rstrip())

    def _startup_error(self, prefix: str) -> str:
        detail = "\n".join(self._stderr_tail)
        return f"{prefix}{f': {detail}' if detail else ''}"

    @staticmethod
    def _allocate_loopback_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            return int(probe.getsockname()[1])
