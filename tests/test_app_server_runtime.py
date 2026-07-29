from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

from codex_thread_console.app_server_runtime import (
    AppServerRuntime,
    installed_codex_path,
    validate_codex_binary,
)
import pytest


def test_sdk_proxy_targets_shared_endpoint(tmp_path) -> None:
    runtime = AppServerRuntime(tmp_path, codex_bin=Path("/tmp/codex"))
    runtime.port = 43123

    assert runtime.remote_url == "ws://127.0.0.1:43123"
    assert runtime.proxy_args == (
        sys.executable,
        "-m",
        "codex_thread_console.ws_stdio_proxy",
        "ws://127.0.0.1:43123",
    )
    assert runtime.sdk_config().launch_args_override == runtime.proxy_args
    assert (
        runtime.sdk_config().env["CODEX_CONSOLE_TAP_TOKEN"]
        == runtime._tap_token
    )


def test_sdk_proxy_includes_notification_tap_when_started(tmp_path) -> None:
    runtime = AppServerRuntime(tmp_path, codex_bin=Path("/tmp/codex"))
    runtime.port = 43123
    runtime.tap_port = 43124

    assert runtime.proxy_args[-2:] == (
        "ws://127.0.0.1:43123",
        "43124",
    )


@pytest.mark.asyncio
async def test_notification_tap_authenticates_and_queues_lifecycle_event(
    tmp_path,
) -> None:
    runtime = AppServerRuntime(tmp_path, codex_bin=Path("/tmp/codex"))
    await runtime._start_notification_tap()
    assert runtime.tap_port is not None
    _reader, writer = await asyncio.open_connection(
        "127.0.0.1", runtime.tap_port
    )
    writer.write(runtime._tap_token.encode() + b"\n")
    writer.write(
        json.dumps(
            {
                "method": "turn/started",
                "params": {
                    "threadId": "thread-1",
                    "turn": {"id": "turn-1"},
                },
            }
        ).encode()
        + b"\n"
    )
    await writer.drain()

    notification = await asyncio.wait_for(
        runtime.next_notification(), timeout=1
    )

    assert notification["method"] == "turn/started"
    assert notification["params"]["turn"]["id"] == "turn-1"
    writer.close()
    await writer.wait_closed()
    await runtime.close()


def test_explicit_binary_is_not_replaced(tmp_path) -> None:
    binary = tmp_path / "codex"
    binary.touch()
    runtime = AppServerRuntime(tmp_path, codex_bin=binary)

    assert runtime.codex_bin == binary.resolve()
    assert runtime.codex_bin_source == "explicit"


def test_installed_binary_falls_back_to_sdk_bundle(monkeypatch) -> None:
    monkeypatch.setattr(
        "codex_thread_console.app_server_runtime.shutil.which",
        lambda _name: None,
    )
    fallback = Path("/tmp/sdk-bundled-codex")
    monkeypatch.setattr(
        "codex_thread_console.app_server_runtime.bundled_codex_path",
        lambda: fallback,
    )

    binary, source = installed_codex_path()

    assert binary == fallback.resolve()
    assert source == "sdk-bundled"


def test_path_binary_inside_chatgpt_app_is_ignored(monkeypatch) -> None:
    desktop_binary = (
        "/Applications/ChatGPT.app/Contents/Resources/codex"
    )
    fallback = Path("/tmp/sdk-bundled-codex")
    monkeypatch.setattr(
        "codex_thread_console.app_server_runtime.shutil.which",
        lambda _name: desktop_binary,
    )
    monkeypatch.setattr(
        "codex_thread_console.app_server_runtime.bundled_codex_path",
        lambda: fallback,
    )

    binary, source = installed_codex_path()

    assert binary == fallback.resolve()
    assert source == "sdk-bundled"


def test_explicit_chatgpt_app_binary_is_rejected() -> None:
    with pytest.raises(RuntimeError, match="refusing to use"):
        validate_codex_binary(
            Path("/Applications/ChatGPT.app/Contents/Resources/codex")
        )
