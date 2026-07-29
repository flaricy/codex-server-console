from __future__ import annotations

import asyncio
import itertools
from collections import defaultdict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class _Mutation:
    sequence: int
    name: str
    operation: Callable[[], Awaitable[Any]]
    future: asyncio.Future[Any]


class MutationSequencer:
    """FIFO serialization for mutations targeting the same Codex thread."""

    def __init__(self) -> None:
        self._counter = itertools.count(1)
        self._queues: defaultdict[str, asyncio.Queue[_Mutation]] = defaultdict(
            asyncio.Queue
        )
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._guard = asyncio.Lock()
        self._closed = False

    async def submit(
        self,
        thread_id: str,
        name: str,
        operation: Callable[[], Awaitable[T]],
    ) -> tuple[int, T]:
        if self._closed:
            raise RuntimeError("mutation sequencer is closed")
        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()
        async with self._guard:
            if self._closed:
                raise RuntimeError("mutation sequencer is closed")
            sequence = next(self._counter)
            self._queues[thread_id].put_nowait(
                _Mutation(sequence, name, operation, future)
            )
            if thread_id not in self._workers:
                self._workers[thread_id] = asyncio.create_task(
                    self._run(thread_id)
                )
        return sequence, await future

    async def close(self) -> None:
        async with self._guard:
            self._closed = True
            workers = list(self._workers.values())
            self._workers.clear()
        for worker in workers:
            worker.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)

    async def _run(self, thread_id: str) -> None:
        queue = self._queues[thread_id]
        try:
            while True:
                mutation = await queue.get()
                if mutation.future.cancelled():
                    queue.task_done()
                else:
                    try:
                        result = await mutation.operation()
                    except BaseException as exc:
                        if not mutation.future.done():
                            mutation.future.set_exception(exc)
                    else:
                        if not mutation.future.done():
                            mutation.future.set_result(result)
                    finally:
                        queue.task_done()
                async with self._guard:
                    if queue.empty():
                        self._workers.pop(thread_id, None)
                        self._queues.pop(thread_id, None)
                        return
        except asyncio.CancelledError:
            while not queue.empty():
                mutation = queue.get_nowait()
                if not mutation.future.done():
                    mutation.future.set_exception(
                        RuntimeError("mutation sequencer stopped")
                    )
            raise
