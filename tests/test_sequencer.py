from __future__ import annotations

import asyncio

import pytest

from codex_thread_console.sequencer import MutationSequencer


@pytest.mark.asyncio
async def test_mutations_are_fifo_per_thread() -> None:
    sequencer = MutationSequencer()
    first_started = asyncio.Event()
    release_first = asyncio.Event()
    order: list[str] = []

    async def first() -> str:
        order.append("first:start")
        first_started.set()
        await release_first.wait()
        order.append("first:end")
        return "one"

    async def second() -> str:
        order.append("second")
        return "two"

    first_task = asyncio.create_task(sequencer.submit("thread", "first", first))
    await first_started.wait()
    second_task = asyncio.create_task(
        sequencer.submit("thread", "second", second)
    )
    await asyncio.sleep(0)
    assert order == ["first:start"]

    release_first.set()
    assert await first_task == (1, "one")
    assert await second_task == (2, "two")
    assert order == ["first:start", "first:end", "second"]
    await sequencer.close()


@pytest.mark.asyncio
async def test_different_threads_do_not_block_each_other() -> None:
    sequencer = MutationSequencer()
    release = asyncio.Event()
    second_finished = asyncio.Event()

    async def blocked() -> None:
        await release.wait()

    async def independent() -> None:
        second_finished.set()

    blocked_task = asyncio.create_task(
        sequencer.submit("thread-a", "blocked", blocked)
    )
    await asyncio.sleep(0)
    independent_task = asyncio.create_task(
        sequencer.submit("thread-b", "independent", independent)
    )
    await asyncio.wait_for(second_finished.wait(), timeout=1)
    release.set()
    await blocked_task
    await independent_task
    await sequencer.close()
