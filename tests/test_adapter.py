from __future__ import annotations

from types import SimpleNamespace

import pytest

from codex_thread_console.adapter import CodexAdapter


class FakeSdkThread:
    def __init__(self) -> None:
        self.body = None
        self.options = None

    async def turn(self, body, **options):
        self.body = body
        self.options = options
        return SimpleNamespace(id="turn-1")


class FakeSdk:
    def __init__(self) -> None:
        self.thread = FakeSdkThread()
        self.resume_options = None

    async def thread_resume(self, _thread_id, **options):
        self.resume_options = options
        return self.thread


@pytest.mark.asyncio
async def test_adapter_maps_stable_turn_options_to_official_sdk_types() -> None:
    sdk = FakeSdk()
    adapter = CodexAdapter(client=sdk)

    handle = await adapter.start_turn(
        "thread-1",
        "structured work",
        {
            "cwd": "/workspace/nested",
            "effort": "high",
            "model": "gpt-test",
            "output_schema": {"type": "object"},
            "personality": "pragmatic",
            "service_tier": "priority",
            "summary": "concise",
        },
    )

    assert handle.id == "turn-1"
    assert sdk.thread.body == "structured work"
    assert sdk.thread.options["effort"].value == "high"
    assert sdk.thread.options["personality"].value == "pragmatic"
    assert sdk.thread.options["summary"].root.value == "concise"
    assert sdk.thread.options["output_schema"] == {"type": "object"}
    assert sdk.thread.options["service_tier"] == "priority"
