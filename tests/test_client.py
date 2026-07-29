from __future__ import annotations

import json
from contextlib import asynccontextmanager

import httpx
import pytest
import websockets

from codex_thread_console.client import (
    AsyncConsoleClient,
    ConsoleAPIError,
    EventStreamGapError,
    TurnOutcome,
)


class FakeEventSocket:
    def __init__(self, events: list[dict[str, object] | BaseException]) -> None:
        self.events = list(events)

    async def recv(self) -> str:
        event = self.events.pop(0)
        if isinstance(event, BaseException):
            raise event
        return json.dumps(event)


class FakeEventClient(AsyncConsoleClient):
    def __init__(
        self,
        events: list[dict[str, object]],
        *,
        transport: httpx.AsyncBaseTransport,
    ) -> None:
        super().__init__(token="secret", transport=transport)
        self.fake_socket = FakeEventSocket(events)

    @asynccontextmanager
    async def _event_connection(self, since_event_id=None):
        self.since_event_id = since_event_id
        yield self.fake_socket


class SequencedFakeEventClient(FakeEventClient):
    def __init__(
        self,
        event_batches: list[list[dict[str, object] | BaseException]],
        *,
        transport: httpx.AsyncBaseTransport,
    ) -> None:
        super().__init__([], transport=transport)
        self.fake_sockets = [
            FakeEventSocket(events) for events in event_batches
        ]
        self.connection_cursors: list[int | None] = []

    @asynccontextmanager
    async def _event_connection(self, since_event_id=None):
        self.connection_cursors.append(since_event_id)
        yield self.fake_sockets.pop(0)


@pytest.mark.asyncio
async def test_typed_http_methods_use_token_and_filters() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["x-console-token"] == "secret"
        assert request.url.params["archived"] == "false"
        assert request.url.params["created_here"] == "true"
        return httpx.Response(
            200,
            json={
                "threads": [
                    {
                        "id": "thread-1",
                        "created_here": True,
                    }
                ]
            },
        )

    async with AsyncConsoleClient(
        token="secret",
        transport=httpx.MockTransport(handler),
    ) as client:
        rows = await client.list_threads(created_here=True)

    assert rows == [{"id": "thread-1", "created_here": True}]


@pytest.mark.asyncio
async def test_api_error_preserves_code_and_status() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            json={
                "error": {
                    "code": "thread_busy",
                    "message": "thread is busy",
                }
            },
        )

    async with AsyncConsoleClient(
        token="secret",
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(ConsoleAPIError) as caught:
            await client.interrupt("thread-1")

    assert caught.value.code == "thread_busy"
    assert caught.value.status == 409


@pytest.mark.asyncio
async def test_wait_for_idle_accepts_not_loaded_history() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/status")
        return httpx.Response(
            200,
            json={
                "thread": {
                    "id": "thread-1",
                    "status": "notLoaded",
                },
                "ownership": "idle",
                "queue": [],
            },
        )

    async with AsyncConsoleClient(
        token="secret",
        transport=httpx.MockTransport(handler),
    ) as client:
        result = await client.wait_for_idle("thread-1", timeout=0.1)

    assert result["thread"]["status"] == "notLoaded"


@pytest.mark.asyncio
async def test_thread_facade_sends_stable_sdk_options() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        assert payload == {
            "text": "return JSON",
            "effort": "high",
            "model": "gpt-test",
            "output_schema": {"type": "object"},
        }
        return httpx.Response(
            200,
            json={
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "state": "running",
                "options": {key: value for key, value in payload.items() if key != "text"},
            },
        )

    async with AsyncConsoleClient(
        token="secret",
        transport=httpx.MockTransport(handler),
    ) as client:
        accepted = await client.thread("thread-1").send(
            "return JSON",
            options={
                "effort": "high",
                "model": "gpt-test",
                "output_schema": {"type": "object"},
            },
        )

    assert accepted["turn_id"] == "turn-1"


def test_turn_outcome_decodes_structured_response() -> None:
    outcome = TurnOutcome(
        thread_id="thread-1",
        turn_id="turn-1",
        final_response='{"answer": 42}',
        error=None,
    )

    assert outcome.json() == {"answer": 42}


@pytest.mark.asyncio
async def test_send_and_wait_returns_matching_final_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/messages/send")
        return httpx.Response(
            200,
            json={
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "state": "running",
            },
        )

    events = [
        {"type": "event_stream_ready", "latest_event_id": 0},
        {"type": "heartbeat"},
        {
            "type": "turn_finished",
            "thread_id": "other-thread",
            "turn_id": "other-turn",
        },
        {
            "type": "turn_finished",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "final_response": "done",
            "error": None,
        },
    ]
    async with FakeEventClient(
        events,
        transport=httpx.MockTransport(handler),
    ) as client:
        outcome = await client.send_and_wait("thread-1", "work")

    assert outcome.turn_id == "turn-1"
    assert outcome.final_response == "done"
    assert outcome.queue_id is None


@pytest.mark.asyncio
async def test_send_and_wait_follows_queued_turn() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "thread_id": "thread-1",
                "queue_id": 7,
                "state": "queued",
            },
        )

    events = [
        {"type": "event_stream_ready", "latest_event_id": 0},
        {
            "type": "turn_started",
            "thread_id": "thread-1",
            "turn_id": "turn-queued",
            "queue_id": 7,
        },
        {
            "type": "turn_finished",
            "thread_id": "thread-1",
            "turn_id": "turn-queued",
            "final_response": "queued done",
            "error": None,
        },
    ]
    async with FakeEventClient(
        events,
        transport=httpx.MockTransport(handler),
    ) as client:
        outcome = await client.send_and_wait("thread-1", "work")

    assert outcome.turn_id == "turn-queued"
    assert outcome.final_response == "queued done"
    assert outcome.queue_id == 7


@pytest.mark.asyncio
async def test_event_controls_survive_thread_filter() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP should not be used")

    events = [
        {"type": "event_stream_ready", "latest_event_id": 4},
        {
            "type": "turn_started",
            "thread_id": "thread-1",
            "turn_id": "turn-1",
            "event_id": 5,
        },
    ]
    async with FakeEventClient(
        events,
        transport=httpx.MockTransport(handler),
    ) as client:
        stream = client.events(thread_id="thread-1", since_event_id=3)
        ready = await anext(stream)
        started = await anext(stream)
        await stream.aclose()

    assert ready["type"] == "event_stream_ready"
    assert started["turn_id"] == "turn-1"
    assert client.since_event_id == 3


@pytest.mark.asyncio
async def test_send_and_wait_reports_event_stream_gap() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/messages/send"):
            return httpx.Response(
                200,
                json={
                    "thread_id": "thread-1",
                    "turn_id": "turn-1",
                    "state": "running",
                },
            )
        if request.url.path.endswith("/status"):
            return httpx.Response(
                200,
                json={
                    "thread": {"id": "thread-1", "status": "idle"},
                    "ownership": "idle",
                    "queue": [],
                },
            )
        raise AssertionError(f"unexpected request: {request.url}")

    async with FakeEventClient(
        [
            {
                "type": "event_stream_ready",
                "latest_event_id": 0,
            },
            {
                "type": "resync_required",
                "reason": "subscriber_overflow",
                "latest_event_id": 900,
            }
        ],
        transport=httpx.MockTransport(handler),
    ) as client:
        with pytest.raises(EventStreamGapError) as caught:
            await client.send_and_wait("thread-1", "work")

    assert caught.value.code == "event_stream_gap"


@pytest.mark.asyncio
async def test_send_and_wait_replays_after_event_disconnect() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/messages/send")
        return httpx.Response(
            200,
            json={
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "state": "running",
            },
        )

    async with SequencedFakeEventClient(
        [
            [
                {"type": "event_stream_ready", "latest_event_id": 10},
                websockets.ConnectionClosedOK(None, None),
            ],
            [
                {
                    "type": "turn_finished",
                    "thread_id": "thread-1",
                    "turn_id": "turn-1",
                    "final_response": "replayed",
                    "error": None,
                    "event_id": 11,
                }
            ],
        ],
        transport=httpx.MockTransport(handler),
    ) as client:
        outcome = await client.send_and_wait("thread-1", "work")

    assert outcome.final_response == "replayed"
    assert client.connection_cursors == [None, 10]


@pytest.mark.asyncio
async def test_send_and_wait_reconnects_before_sending_if_not_ready() -> None:
    requests = 0

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        assert request.url.path.endswith("/messages/send")
        return httpx.Response(
            200,
            json={
                "thread_id": "thread-1",
                "turn_id": "turn-1",
                "state": "running",
            },
        )

    async with SequencedFakeEventClient(
        [
            [websockets.ConnectionClosedOK(None, None)],
            [
                {"type": "event_stream_ready", "latest_event_id": 20},
                {
                    "type": "turn_finished",
                    "thread_id": "thread-1",
                    "turn_id": "turn-1",
                    "final_response": "done",
                    "error": None,
                    "event_id": 21,
                },
            ],
        ],
        transport=httpx.MockTransport(handler),
    ) as client:
        outcome = await client.send_and_wait("thread-1", "work")

    assert outcome.final_response == "done"
    assert requests == 1
    assert client.connection_cursors == [None, None]


@pytest.mark.asyncio
async def test_events_resume_from_last_delivered_cursor() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        raise AssertionError("HTTP should not be used")

    async with SequencedFakeEventClient(
        [
            [
                {"type": "event_stream_ready", "latest_event_id": 3},
                {
                    "type": "turn_started",
                    "thread_id": "thread-1",
                    "turn_id": "turn-1",
                    "event_id": 4,
                },
                websockets.ConnectionClosedOK(None, None),
            ],
            [
                {"type": "event_stream_ready", "latest_event_id": 4},
                {
                    "type": "turn_finished",
                    "thread_id": "thread-1",
                    "turn_id": "turn-1",
                    "event_id": 5,
                },
            ],
        ],
        transport=httpx.MockTransport(handler),
    ) as client:
        stream = client.events(reconnect_delay=0)
        assert (await anext(stream))["type"] == "event_stream_ready"
        assert (await anext(stream))["event_id"] == 4
        assert (await anext(stream))["type"] == "event_stream_ready"
        assert (await anext(stream))["event_id"] == 5
        await stream.aclose()

    assert client.connection_cursors == [None, 4]
