from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import httpx
import websockets

from .config import default_data_dir
from .turns import TurnOptions, normalize_turn_options


class ConsoleAPIError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        code: str = "console_api_error",
        status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.status = status


class TurnFailedError(ConsoleAPIError):
    pass


class EventStreamGapError(ConsoleAPIError):
    """The turn completed, but its final event fell outside the replay window."""


@dataclass(frozen=True, slots=True)
class TurnOutcome:
    thread_id: str
    turn_id: str
    final_response: str | None
    error: str | None
    queue_id: int | None = None

    def json(self) -> Any:
        """Decode a structured final response produced with output_schema."""
        if self.final_response is None:
            raise ValueError("turn has no final response")
        return json.loads(self.final_response)


def _event_thread_id(event: dict[str, Any]) -> str | None:
    value = event.get("thread_id")
    if isinstance(value, str):
        return value
    item = event.get("item")
    if isinstance(item, dict) and isinstance(item.get("thread_id"), str):
        return str(item["thread_id"])
    return None


class AsyncConsoleClient:
    """Thin workflow client for the already-running shared control plane.

    The server remains the only owner of the official Python SDK and app-server.
    Workflow processes use this client instead of launching a second Codex
    runtime, so Web TUI and automation always observe the same threads.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8765",
        *,
        token: str | None = None,
        token_file: Path | None = None,
        timeout: float = 30.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "http"
            or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("base_url must be a loopback http:// URL")
        self.base_url = base_url.rstrip("/")
        self._parsed_url = urlsplit(self.base_url)
        self._token = token
        self._token_file = token_file or default_data_dir() / "session-token"
        self._timeout = timeout
        self._transport = transport
        self._http: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "AsyncConsoleClient":
        await self._ensure_http()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    def _load_token(self) -> str:
        if self._token:
            return self._token
        try:
            token = self._token_file.expanduser().read_text(
                encoding="utf-8"
            ).strip()
        except OSError as exc:
            raise ConsoleAPIError(
                f"cannot read console token: {exc}",
                code="token_unavailable",
            ) from exc
        if not token:
            raise ConsoleAPIError(
                "console token file is empty",
                code="token_unavailable",
            )
        self._token = token
        return token

    async def _ensure_http(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                base_url=self.base_url,
                headers={"X-Console-Token": self._load_token()},
                timeout=self._timeout,
                transport=self._transport,
            )
        return self._http

    async def close(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, object] | None = None,
        json_body: dict[str, object] | None = None,
    ) -> dict[str, Any]:
        client = await self._ensure_http()
        try:
            response = await client.request(
                method, path, params=params, json=json_body
            )
        except httpx.HTTPError as exc:
            raise ConsoleAPIError(
                f"console request failed: {exc}",
                code="connection_error",
            ) from exc
        try:
            payload = response.json()
        except json.JSONDecodeError as exc:
            raise ConsoleAPIError(
                f"console returned invalid JSON (HTTP {response.status_code})",
                status=response.status_code,
            ) from exc
        if not isinstance(payload, dict):
            raise ConsoleAPIError(
                f"console returned an invalid payload (HTTP {response.status_code})",
                status=response.status_code,
            )
        if response.is_error:
            error = payload.get("error")
            detail = error if isinstance(error, dict) else {}
            raise ConsoleAPIError(
                str(detail.get("message") or f"HTTP {response.status_code}"),
                code=str(detail.get("code") or "console_api_error"),
                status=response.status_code,
            )
        return payload

    async def health(self) -> dict[str, Any]:
        return await self._request("GET", "/api/health")

    async def list_threads(
        self,
        *,
        archived: bool = False,
        created_here: bool = False,
    ) -> list[dict[str, Any]]:
        payload = await self._request(
            "GET",
            "/api/threads",
            params={"archived": archived, "created_here": created_here},
        )
        return list(payload.get("threads", []))

    async def snapshot(
        self,
        *,
        archived: bool = False,
        created_here: bool = False,
    ) -> dict[str, Any]:
        return await self._request(
            "GET",
            "/api/snapshot",
            params={"archived": archived, "created_here": created_here},
        )

    async def create_thread(
        self,
        *,
        cwd: str | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            "/api/threads",
            json_body={"cwd": cwd, "name": name},
        )
        return dict(payload["thread"])

    def thread(self, thread_id: str) -> "AsyncThreadController":
        return AsyncThreadController(self, thread_id)

    async def rename(self, thread_id: str, name: str) -> dict[str, Any]:
        payload = await self._request(
            "PATCH",
            f"/api/threads/{thread_id}",
            json_body={"name": name},
        )
        return dict(payload["thread"])

    async def status(self, thread_id: str) -> dict[str, Any]:
        return await self._request("GET", f"/api/threads/{thread_id}/status")

    async def send(
        self,
        thread_id: str,
        text: str,
        *,
        options: TurnOptions | None = None,
    ) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/threads/{thread_id}/messages/send",
            json_body={
                "text": text,
                **normalize_turn_options(options),
            },
        )

    async def steer(self, thread_id: str, text: str) -> dict[str, Any]:
        return await self._request(
            "POST",
            f"/api/threads/{thread_id}/messages/steer",
            json_body={"text": text},
        )

    async def queue(
        self,
        thread_id: str,
        text: str,
        *,
        options: TurnOptions | None = None,
    ) -> dict[str, Any]:
        payload = await self._request(
            "POST",
            f"/api/threads/{thread_id}/messages/queue",
            json_body={
                "text": text,
                **normalize_turn_options(options),
            },
        )
        return dict(payload["item"])

    async def interrupt(self, thread_id: str) -> dict[str, Any]:
        return await self._request(
            "POST", f"/api/threads/{thread_id}/interrupt"
        )

    async def archive(self, thread_id: str) -> dict[str, Any]:
        return await self._request(
            "POST", f"/api/threads/{thread_id}/archive"
        )

    async def restore(self, thread_id: str) -> dict[str, Any]:
        payload = await self._request(
            "POST", f"/api/threads/{thread_id}/restore"
        )
        return dict(payload["thread"])

    async def delete(self, thread_id: str) -> dict[str, Any]:
        return await self._request(
            "DELETE", f"/api/threads/{thread_id}"
        )

    async def list_queue(
        self, thread_id: str | None = None
    ) -> list[dict[str, Any]]:
        payload = await self._request(
            "GET",
            "/api/queue",
            params={"thread_id": thread_id} if thread_id else None,
        )
        return list(payload.get("queue", []))

    async def cancel_queue(self, item_id: int) -> dict[str, Any]:
        payload = await self._request("DELETE", f"/api/queue/{item_id}")
        return dict(payload["item"])

    async def retry_queue(self, item_id: int) -> dict[str, Any]:
        payload = await self._request("POST", f"/api/queue/{item_id}/retry")
        return dict(payload["item"])

    def _event_url(self, since_event_id: int | None = None) -> str:
        return urlunsplit(
            (
                "ws",
                self._parsed_url.netloc,
                f"{self._parsed_url.path.rstrip('/')}/ws/events",
                (
                    f"since={since_event_id}"
                    if since_event_id is not None
                    else ""
                ),
                "",
            )
        )

    @property
    def _origin(self) -> str:
        return urlunsplit(
            (
                self._parsed_url.scheme,
                self._parsed_url.netloc,
                "",
                "",
                "",
            )
        )

    @asynccontextmanager
    async def _event_connection(
        self, since_event_id: int | None = None
    ) -> AsyncIterator[Any]:
        async with websockets.connect(
            self._event_url(since_event_id),
            origin=self._origin,
            additional_headers={"X-Console-Token": self._load_token()},
            open_timeout=self._timeout,
        ) as socket:
            yield socket

    async def events(
        self,
        *,
        thread_id: str | None = None,
        since_event_id: int | None = None,
        reconnect: bool = True,
        reconnect_delay: float = 0.25,
    ) -> AsyncIterator[dict[str, Any]]:
        cursor = since_event_id
        delay = max(0.0, reconnect_delay)
        while True:
            try:
                async with self._event_connection(cursor) as socket:
                    while True:
                        event = json.loads(await socket.recv())
                        event_id = event.get("event_id")
                        if isinstance(event_id, int):
                            cursor = max(cursor or 0, event_id)
                        elif event.get("type") == "event_stream_ready":
                            latest = event.get("latest_event_id")
                            if isinstance(latest, int):
                                cursor = max(cursor or 0, latest)
                        if event.get("type") == "heartbeat":
                            continue
                        if (
                            event.get("type")
                            in {"event_stream_ready", "resync_required"}
                            or thread_id is None
                            or _event_thread_id(event) == thread_id
                        ):
                            yield event
            except asyncio.CancelledError:
                raise
            except (
                websockets.ConnectionClosed,
                OSError,
                asyncio.TimeoutError,
            ):
                if not reconnect:
                    raise
                if delay:
                    await asyncio.sleep(delay)

    async def wait_for_idle(
        self,
        thread_id: str,
        *,
        timeout: float = 300.0,
        poll_interval: float = 0.25,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        while True:
            current = await self.status(thread_id)
            thread = current.get("thread")
            if (
                current.get("ownership") == "idle"
                and isinstance(thread, dict)
                and thread.get("status") == "idle"
            ):
                return current
            if time.monotonic() >= deadline:
                raise TimeoutError(f"thread {thread_id} did not become idle")
            await asyncio.sleep(poll_interval)

    async def send_and_wait(
        self,
        thread_id: str,
        text: str,
        *,
        options: TurnOptions | None = None,
        timeout: float = 300.0,
        raise_on_error: bool = True,
    ) -> TurnOutcome:
        async def run() -> TurnOutcome:
            cursor: int | None = None
            target_turn: Any = None
            target_queue: Any = None

            async def handle_event(event: dict[str, Any]) -> TurnOutcome | None:
                nonlocal cursor, target_turn
                event_id = event.get("event_id")
                if isinstance(event_id, int):
                    cursor = max(cursor or 0, event_id)
                event_type = event.get("type")
                if event_type == "heartbeat":
                    return None
                if event_type == "event_stream_ready":
                    latest = event.get("latest_event_id")
                    if isinstance(latest, int):
                        cursor = max(cursor or 0, latest)
                    return None
                if event_type == "resync_required":
                    await self.wait_for_idle(thread_id, timeout=timeout)
                    raise EventStreamGapError(
                        "turn became idle after an event-stream gap; "
                        "the final response cannot be recovered safely",
                        code="event_stream_gap",
                    )
                if _event_thread_id(event) != thread_id:
                    return None
                if (
                    event_type == "queue_indeterminate"
                    and target_queue is not None
                    and event.get("item_id") == target_queue
                ):
                    raise TurnFailedError(
                        str(event.get("error") or "queued turn is indeterminate"),
                        code="queue_indeterminate",
                    )
                if (
                    event_type == "turn_started"
                    and target_queue is not None
                    and event.get("queue_id") == target_queue
                ):
                    target_turn = event.get("turn_id")
                    return None
                if (
                    event_type == "turn_finished"
                    and target_turn is not None
                    and event.get("turn_id") == target_turn
                ):
                    outcome = TurnOutcome(
                        thread_id=thread_id,
                        turn_id=str(target_turn),
                        final_response=event.get("final_response"),
                        error=event.get("error"),
                        queue_id=(
                            int(target_queue)
                            if target_queue is not None
                            else None
                        ),
                    )
                    if outcome.error and raise_on_error:
                        raise TurnFailedError(
                            outcome.error,
                            code="turn_failed",
                        )
                    return outcome
                return None

            async def receive(socket: Any) -> TurnOutcome:
                while True:
                    event = json.loads(await socket.recv())
                    outcome = await handle_event(event)
                    if outcome is not None:
                        return outcome

            reconnect_errors = (
                websockets.ConnectionClosed,
                OSError,
                asyncio.TimeoutError,
            )
            disconnected = False
            while target_turn is None and target_queue is None:
                try:
                    async with self._event_connection() as socket:
                        # The ready frame proves the server-side subscriber
                        # exists before the HTTP mutation can publish
                        # turn_started.
                        while True:
                            initial = json.loads(await socket.recv())
                            if initial.get("type") == "heartbeat":
                                continue
                            if initial.get("type") == "resync_required":
                                await handle_event(initial)
                            if initial.get("type") != "event_stream_ready":
                                raise ConsoleAPIError(
                                    "event stream did not send its ready frame",
                                    code="invalid_event_stream",
                                )
                            await handle_event(initial)
                            break

                        accepted = await self.send(
                            thread_id,
                            text,
                            options=options,
                        )
                        target_turn = accepted.get("turn_id")
                        target_queue = accepted.get("queue_id")
                        if target_turn is None and target_queue is None:
                            raise ConsoleAPIError(
                                "console accepted a turn without a turn_id "
                                "or queue_id",
                                code="invalid_turn_acceptance",
                            )
                        try:
                            return await receive(socket)
                        except reconnect_errors:
                            disconnected = True
                except reconnect_errors:
                    # No mutation has been sent yet, so reconnecting here
                    # cannot duplicate work.
                    await asyncio.sleep(0.25)
                    continue
                if disconnected:
                    break

            while disconnected:
                try:
                    async with self._event_connection(cursor) as socket:
                        return await receive(socket)
                except reconnect_errors:
                    await asyncio.sleep(0.25)

            raise AssertionError("unreachable event stream state")

        return await asyncio.wait_for(run(), timeout=timeout)


@dataclass(frozen=True, slots=True)
class AsyncThreadController:
    """Thread-bound convenience facade for composable workflows."""

    _client: AsyncConsoleClient
    id: str

    async def status(self) -> dict[str, Any]:
        return await self._client.status(self.id)

    async def rename(self, name: str) -> dict[str, Any]:
        return await self._client.rename(self.id, name)

    async def send(
        self,
        text: str,
        *,
        options: TurnOptions | None = None,
    ) -> dict[str, Any]:
        return await self._client.send(self.id, text, options=options)

    async def send_and_wait(
        self,
        text: str,
        *,
        options: TurnOptions | None = None,
        timeout: float = 300.0,
        raise_on_error: bool = True,
    ) -> TurnOutcome:
        return await self._client.send_and_wait(
            self.id,
            text,
            options=options,
            timeout=timeout,
            raise_on_error=raise_on_error,
        )

    async def steer(self, text: str) -> dict[str, Any]:
        return await self._client.steer(self.id, text)

    async def queue(
        self,
        text: str,
        *,
        options: TurnOptions | None = None,
    ) -> dict[str, Any]:
        return await self._client.queue(self.id, text, options=options)

    async def interrupt(self) -> dict[str, Any]:
        return await self._client.interrupt(self.id)

    async def list_queue(self) -> list[dict[str, Any]]:
        return await self._client.list_queue(self.id)

    async def archive(self) -> dict[str, Any]:
        return await self._client.archive(self.id)

    async def restore(self) -> dict[str, Any]:
        return await self._client.restore(self.id)

    async def delete(self) -> dict[str, Any]:
        return await self._client.delete(self.id)

    async def wait_for_idle(
        self,
        *,
        timeout: float = 300.0,
        poll_interval: float = 0.25,
    ) -> dict[str, Any]:
        return await self._client.wait_for_idle(
            self.id,
            timeout=timeout,
            poll_interval=poll_interval,
        )

    def events(
        self,
        *,
        since_event_id: int | None = None,
        reconnect: bool = True,
    ) -> AsyncIterator[dict[str, Any]]:
        return self._client.events(
            thread_id=self.id,
            since_event_id=since_event_id,
            reconnect=reconnect,
        )
