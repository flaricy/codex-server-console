from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any


class EventBroker:
    """Fan out ordered events and retain a bounded reconnect window."""

    def __init__(self, capacity: int = 256) -> None:
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self._capacity = capacity
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()
        self._history: deque[dict[str, Any]] = deque(maxlen=capacity)
        self._latest_event_id = 0

    @property
    def latest_event_id(self) -> int:
        return self._latest_event_id

    def publish(self, event: dict[str, Any]) -> None:
        self._latest_event_id += 1
        event = {
            **event,
            "event_id": self._latest_event_id,
            "published_at": datetime.now(timezone.utc).isoformat(),
        }
        self._history.append(event)
        for queue in tuple(self._subscribers):
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                while not queue.empty():
                    queue.get_nowait()
                queue.put_nowait(self._resync_event("subscriber_overflow"))

    def _resync_event(self, reason: str) -> dict[str, Any]:
        return {
            "type": "resync_required",
            "reason": reason,
            "latest_event_id": self._latest_event_id,
        }

    @asynccontextmanager
    async def subscribe(
        self, since_event_id: int | None = None
    ) -> AsyncIterator[asyncio.Queue[dict[str, Any]]]:
        # One extra slot leaves room for the ready control frame after a full
        # replay window. Registration and replay are synchronous, so publish()
        # cannot interleave and create a gap on the current event loop.
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(self._capacity + 1)
        self._subscribers.add(queue)
        try:
            if since_event_id is not None:
                earliest = (
                    int(self._history[0]["event_id"])
                    if self._history
                    else self._latest_event_id + 1
                )
                if since_event_id > self._latest_event_id:
                    queue.put_nowait(self._resync_event("stream_reset"))
                elif since_event_id < earliest - 1:
                    queue.put_nowait(self._resync_event("replay_window_exceeded"))
                else:
                    for event in self._history:
                        if int(event["event_id"]) > since_event_id:
                            queue.put_nowait(event)
            queue.put_nowait(
                {
                    "type": "event_stream_ready",
                    "latest_event_id": self._latest_event_id,
                }
            )
            yield queue
        finally:
            self._subscribers.discard(queue)
