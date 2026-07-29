from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
import signal
import shutil
import socket
import sys
from collections import deque
from pathlib import Path
from typing import Any

from codex_cli_bin import bundled_codex_path
from openai_codex import CodexConfig


def _is_chatgpt_app_binary(path: Path) -> bool:
    """Return True for binaries shipped inside ChatGPT.app.

    The console must never depend on, inspect, or launch the desktop app's
    private Codex runtime.  Resolve symlinks before checking so a PATH entry
    cannot accidentally bypass that boundary.
    """
    try:
        resolved = path.expanduser().resolve()
    except OSError:
        resolved = path.expanduser().absolute()
    chatgpt_resources = Path(
        "/Applications/ChatGPT.app/Contents/Resources"
    )
    try:
        return resolved.is_relative_to(chatgpt_resources)
    except ValueError:
        return False


def validate_codex_binary(path: Path) -> Path:
    resolved = path.expanduser().resolve()
    if _is_chatgpt_app_binary(resolved):
        raise RuntimeError(
            "refusing to use the Codex binary bundled inside ChatGPT.app; "
            "install Codex on PATH or use the official Python SDK bundle"
        )
    return resolved


def installed_codex_path() -> tuple[Path, str]:
    """Resolve a public Codex CLI without touching ChatGPT.app internals."""
    path_codex = shutil.which("codex")
    if path_codex and not _is_chatgpt_app_binary(Path(path_codex)):
        return validate_codex_binary(Path(path_codex)), "path"
    return validate_codex_binary(bundled_codex_path()), "sdk-bundled"


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
            self.codex_bin = validate_codex_binary(codex_bin)
            self.codex_bin_source = "explicit"
        self.startup_timeout = startup_timeout
        self.codex_version = "unknown"
        self.port: int | None = None
        self.process: asyncio.subprocess.Process | None = None
        self._stderr_task: asyncio.Task[None] | None = None
        self._stderr_tail: deque[str] = deque(maxlen=100)
        self.tap_port: int | None = None
        self._tap_token = secrets.token_urlsafe(32)
        self._tap_server: asyncio.AbstractServer | None = None
        self._tap_writers: set[asyncio.StreamWriter] = set()
        self._notification_queue: asyncio.Queue[dict[str, Any]] = (
            asyncio.Queue(maxsize=2048)
        )

    @property
    def remote_url(self) -> str:
        if self.port is None:
            raise RuntimeError("app-server has not started")
        return f"ws://127.0.0.1:{self.port}"

    @property
    def proxy_args(self) -> tuple[str, ...]:
        args = (
            sys.executable,
            "-m",
            "codex_thread_console.ws_stdio_proxy",
            self.remote_url,
        )
        if self.tap_port is not None:
            return (*args, str(self.tap_port))
        return args

    def sdk_config(self) -> CodexConfig:
        return CodexConfig(
            launch_args_override=self.proxy_args,
            env={"CODEX_CONSOLE_TAP_TOKEN": self._tap_token},
        )

    @property
    def notification_tap_clients(self) -> int:
        return len(self._tap_writers)

    async def next_notification(self) -> dict[str, Any]:
        return await self._notification_queue.get()

    async def start(self) -> None:
        if self.process is not None:
            return
        if not self.codex_bin.is_file() or not os.access(self.codex_bin, os.X_OK):
            raise RuntimeError(
                f"Codex binary is not executable: {self.codex_bin}"
            )
        self.codex_version = await self._read_codex_version()
        self.runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        await self._start_notification_tap()
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
        await self._close_notification_tap()
        try:
            self.endpoint_path.unlink()
        except FileNotFoundError:
            pass
        self.port = None

    async def _start_notification_tap(self) -> None:
        if self._tap_server is not None:
            return
        self._tap_server = await asyncio.start_server(
            self._handle_notification_tap,
            "127.0.0.1",
            0,
            limit=256 * 1024,
        )
        socket_info = self._tap_server.sockets
        if not socket_info:
            raise RuntimeError("notification tap did not bind a socket")
        self.tap_port = int(socket_info[0].getsockname()[1])

    async def _close_notification_tap(self) -> None:
        server = self._tap_server
        self._tap_server = None
        if server is not None:
            server.close()
            await server.wait_closed()
        writers = tuple(self._tap_writers)
        self._tap_writers.clear()
        for writer in writers:
            writer.close()
        if writers:
            await asyncio.gather(
                *(writer.wait_closed() for writer in writers),
                return_exceptions=True,
            )
        self.tap_port = None

    async def _handle_notification_tap(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        try:
            raw_token = await asyncio.wait_for(reader.readline(), timeout=2)
            token = raw_token.decode("utf-8", errors="replace").rstrip("\n")
            if not hmac.compare_digest(token, self._tap_token):
                return
            self._tap_writers.add(writer)
            while line := await reader.readline():
                try:
                    notification = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    not isinstance(notification, dict)
                    or not isinstance(notification.get("method"), str)
                    or not isinstance(notification.get("params"), dict)
                ):
                    continue
                try:
                    self._notification_queue.put_nowait(notification)
                except asyncio.QueueFull:
                    while not self._notification_queue.empty():
                        self._notification_queue.get_nowait()
                    self._notification_queue.put_nowait(
                        {
                            "method": "tap/resync_required",
                            "params": {"reason": "notification_overflow"},
                        }
                    )
        except (
            asyncio.IncompleteReadError,
            ConnectionError,
            OSError,
            ValueError,
        ):
            return
        finally:
            self._tap_writers.discard(writer)
            writer.close()
            await asyncio.gather(
                writer.wait_closed(),
                return_exceptions=True,
            )

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
