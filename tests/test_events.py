from __future__ import annotations

import pytest

from codex_thread_console.events import EventBroker


@pytest.mark.asyncio
async def test_event_broker_replays_ordered_events() -> None:
    broker = EventBroker(capacity=3)
    broker.publish({"type": "first"})
    broker.publish({"type": "second"})

    async with broker.subscribe(since_event_id=1) as queue:
        replay = await queue.get()
        ready = await queue.get()

    assert replay["type"] == "second"
    assert replay["event_id"] == 2
    assert replay["published_at"]
    assert ready == {"type": "event_stream_ready", "latest_event_id": 2}


@pytest.mark.asyncio
async def test_event_broker_requires_snapshot_outside_replay_window() -> None:
    broker = EventBroker(capacity=2)
    broker.publish({"type": "one"})
    broker.publish({"type": "two"})
    broker.publish({"type": "three"})

    async with broker.subscribe(since_event_id=0) as queue:
        resync = await queue.get()
        ready = await queue.get()

    assert resync == {
        "type": "resync_required",
        "reason": "replay_window_exceeded",
        "latest_event_id": 3,
    }
    assert ready["latest_event_id"] == 3


@pytest.mark.asyncio
async def test_event_broker_detects_server_restart_cursor() -> None:
    broker = EventBroker()

    async with broker.subscribe(since_event_id=12) as queue:
        resync = await queue.get()

    assert resync["reason"] == "stream_reset"
    assert resync["latest_event_id"] == 0
