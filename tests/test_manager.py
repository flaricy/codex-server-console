from __future__ import annotations

import asyncio

import pytest

from codex_thread_console.config import Settings
from codex_thread_console.errors import ConflictError
from codex_thread_console.manager import Ownership, ThreadManager
from codex_thread_console.store import QueueStore

from .fakes import FakeAdapter


def make_manager(tmp_path):
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    data.mkdir()
    settings = Settings(workspace, data, session_token="test-token")
    adapter = FakeAdapter(workspace)
    store = QueueStore(data / "queue.sqlite3")
    return ThreadManager(adapter, store, settings), adapter


@pytest.mark.asyncio
async def test_send_steer_complete(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    started = await manager.send(thread["id"], "work")
    assert started["state"] == "running"
    result = await manager.steer(thread["id"], "focus")
    assert result["accepted"] is True
    assert adapter.turns[0].steers == ["focus"]
    adapter.turns[0].done.set()
    await asyncio.wait_for(manager._tasks[thread["id"]], timeout=1)
    assert manager._mode(thread["id"]) is Ownership.idle
    manager.store.close()


@pytest.mark.asyncio
async def test_steer_and_interrupt_tui_originated_turn(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    external = adapter.add_external_turn(thread["id"])

    steered = await manager.steer(thread["id"], "change direction")
    assert steered["turn_id"] == external.id
    assert steered["accepted"] is True
    assert external.steers == ["change direction"]

    interrupted = await manager.interrupt(thread["id"])
    assert interrupted["turn_id"] == external.id
    assert interrupted["interrupted"] is True
    assert external.interrupted is True
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_last_pty_detach_leaves_tui_originated_turn_running(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    await manager.reserve_pty(thread["id"])
    external = adapter.add_external_turn(thread["id"])

    await manager.release_pty(thread["id"])

    assert external.interrupted is False
    assert manager._pty_counts[thread["id"]] == 0
    await manager.shutdown()
    assert external.interrupted is True
    manager.store.close()


@pytest.mark.asyncio
async def test_shutdown_interrupts_tui_originated_turn(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    external = adapter.add_external_turn(thread["id"])

    await manager.shutdown()

    assert external.interrupted is True
    manager.store.close()


@pytest.mark.asyncio
async def test_queue_dispatches_immediately_and_in_order(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    first = await manager.queue(thread["id"], "first")
    second = await manager.queue(thread["id"], "second")
    for _ in range(20):
        if adapter.turns:
            break
        await asyncio.sleep(0)
    assert len(adapter.turns) == 1
    rows = manager.store.list(thread["id"])
    assert rows[0]["state"] == "running"
    assert rows[1]["id"] == second["id"]

    adapter.turns[0].done.set()
    for _ in range(40):
        if len(adapter.turns) == 2:
            break
        await asyncio.sleep(0)
    assert len(adapter.turns) == 2
    adapter.turns[1].done.set()
    for _ in range(40):
        if all(
            row["state"] == "done" for row in manager.store.list(thread["id"])
        ):
            break
        await asyncio.sleep(0)
    assert [row["state"] for row in manager.store.list(thread["id"])] == [
        "done",
        "done",
    ]
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_remote_pty_can_observe_sdk_turn(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    await manager.reserve_pty(thread["id"])
    assert manager.store.get_managed(thread["id"])["draft"] == 0
    assert (await manager.status(thread["id"]))["terminal_attached"] is True

    result = await manager.send(thread["id"], "work")
    assert result["state"] == "running"
    with pytest.raises(ConflictError):
        await manager.archive(thread["id"])

    await manager.interrupt(thread["id"])
    await manager.release_pty(thread["id"])
    await asyncio.sleep(0)
    assert adapter.turns[0].interrupted is True
    assert manager._mode(thread["id"]) is Ownership.idle
    assert (await manager.status(thread["id"]))["terminal_attached"] is False
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_concurrent_send_is_started_then_fifo_queued(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")

    first, second = await asyncio.gather(
        manager.send(thread["id"], "first"),
        manager.send(thread["id"], "second"),
    )

    assert first["state"] == "running"
    assert first["mutation_sequence"] == 1
    assert second["state"] == "queued"
    assert second["mutation_sequence"] == 2
    assert len(adapter.turns) == 1

    adapter.turns[0].done.set()
    for _ in range(40):
        if len(adapter.turns) == 2:
            break
        await asyncio.sleep(0)
    assert len(adapter.turns) == 2
    adapter.turns[1].done.set()
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_queued_message_cannot_be_overtaken_by_later_send(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    original_trigger = manager._trigger_dispatch
    manager._trigger_dispatch = lambda _thread_id: None

    first = await manager.queue(thread["id"], "queued first")
    second = await manager.send(thread["id"], "sent second")

    assert first["mutation_sequence"] == 1
    assert second["mutation_sequence"] == 2
    assert second["state"] == "queued"
    assert [
        row["body"] for row in manager.store.list(thread["id"], open_only=True)
    ] == ["queued first", "sent second"]
    assert adapter.turns == []

    manager._trigger_dispatch = original_trigger
    await manager.dispatch_if_idle(thread["id"])
    assert len(adapter.turns) == 1
    assert manager.store.list(thread["id"])[0]["state"] == "running"
    adapter.turns[0].done.set()
    await asyncio.wait_for(manager._tasks[thread["id"]], timeout=1)
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_retried_message_cannot_be_overtaken_by_later_send(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    item = manager.store.enqueue(thread["id"], "retry first")
    claimed = manager.store.claim_next(thread["id"])
    assert claimed is not None
    manager.store.finish(item["id"], "indeterminate", "unknown")
    manager._trigger_dispatch = lambda _thread_id: None

    retried = await manager.retry_queue(item["id"])
    sent = await manager.send(thread["id"], "sent second")

    assert retried["mutation_sequence"] == 1
    assert sent["mutation_sequence"] == 2
    assert sent["state"] == "queued"
    assert [
        row["body"] for row in manager.store.list(thread["id"], open_only=True)
    ] == ["retry first", "sent second"]
    assert adapter.turns == []
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_thread_list_includes_app_server_domain_by_default(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    managed = await manager.create_thread(None, "managed")
    adapter.rows["external-thread"] = {
        "id": "external-thread",
        "name": "unrelated local history",
        "preview": "",
        "cwd": str(adapter.cwd),
        "status": "idle",
        "archived": False,
    }

    default_rows = await manager.list_threads()
    assert {row["id"] for row in default_rows} == {
        managed["id"],
        "external-thread",
    }
    assert next(
        row for row in default_rows if row["id"] == "external-thread"
    )["created_here"] is False
    assert next(
        row for row in default_rows if row["id"] == managed["id"]
    )["created_here"] is True

    created_here = await manager.list_threads(created_here_only=True)
    assert [row["id"] for row in created_here] == [managed["id"]]
    manager.store.close()


@pytest.mark.asyncio
async def test_archive_refuses_pending_queue(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    await manager.queue(thread["id"], "queued")
    for _ in range(20):
        if adapter.turns:
            break
        await asyncio.sleep(0)
    adapter.turns[0].done.set()
    for _ in range(40):
        if manager.store.list(thread["id"])[0]["state"] == "done":
            break
        await asyncio.sleep(0)
    indeterminate = manager.store.enqueue(thread["id"], "unresolved")
    claimed = manager.store.claim_next(thread["id"])
    assert claimed and claimed["id"] == indeterminate["id"]
    manager.store.finish(indeterminate["id"], "indeterminate", "crash")
    with pytest.raises(ConflictError, match="cancel or resolve"):
        await manager.archive(thread["id"])
    manager.store.close()


@pytest.mark.asyncio
async def test_archive_and_rename_refuse_tui_started_turn(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    adapter.rows[thread["id"]]["status"] = "active"

    with pytest.raises(ConflictError, match="cannot archive"):
        await manager.archive(thread["id"])
    with pytest.raises(ConflictError, match="cannot rename"):
        await manager.rename(thread["id"], "renamed")

    assert thread["id"] not in adapter.archived
    assert adapter.rows[thread["id"]]["name"] == "demo"
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_delete_is_permanent_and_removes_local_state(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    manager.store.enqueue(thread["id"], "discard with deleted session")

    result = await manager.delete(thread["id"])

    assert result["deleted"] is True
    assert thread["id"] not in adapter.rows
    assert manager.store.get_managed(thread["id"]) is None
    assert manager.store.list(thread["id"]) == []
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_interrupted_queued_turn_is_failed_not_done(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    item = await manager.queue(thread["id"], "long work")
    for _ in range(20):
        if thread["id"] in manager._tasks:
            break
        await asyncio.sleep(0)
    turn_task = manager._tasks[thread["id"]]
    await manager.interrupt(thread["id"])
    await asyncio.wait_for(turn_task, timeout=1)
    row = manager.store.list(thread["id"])[0]
    assert row["id"] == item["id"]
    assert row["state"] == "failed"
    assert row["error"] == "turn ended with status interrupted"
    manager.store.close()


@pytest.mark.asyncio
async def test_archived_draft_rejects_direct_id_mutations(tmp_path) -> None:
    manager, _adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "draft")
    await manager.archive(thread["id"])

    with pytest.raises(ConflictError, match="restore the thread"):
        await manager.send(thread["id"], "work")
    with pytest.raises(ConflictError, match="restore the thread"):
        await manager.queue(thread["id"], "work")
    with pytest.raises(ConflictError, match="restore the thread"):
        await manager.rename(thread["id"], "renamed")
    with pytest.raises(ConflictError, match="restore the thread"):
        await manager.reserve_pty(thread["id"])
    manager.store.close()


@pytest.mark.asyncio
async def test_queue_and_archive_are_serialized_by_thread_lock(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    read_started = asyncio.Event()
    read_release = asyncio.Event()
    original_read = adapter.read_thread

    async def blocked_read(thread_id, *, archived=False):
        read_started.set()
        await read_release.wait()
        return await original_read(thread_id, archived=archived)

    adapter.read_thread = blocked_read
    queue_task = asyncio.create_task(manager.queue(thread["id"], "queued"))
    await asyncio.wait_for(read_started.wait(), timeout=1)
    archive_task = asyncio.create_task(manager.archive(thread["id"]))
    await asyncio.sleep(0)
    assert not archive_task.done()
    read_release.set()
    await queue_task
    with pytest.raises(ConflictError, match="cancel or resolve"):
        await archive_task
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_shutdown_waits_for_inflight_dispatch_start(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    start_entered = asyncio.Event()
    start_release = asyncio.Event()
    original_start = adapter.start_turn

    async def blocked_start(thread_id, body):
        start_entered.set()
        await start_release.wait()
        return await original_start(thread_id, body)

    adapter.start_turn = blocked_start
    await manager.queue(thread["id"], "queued")
    await asyncio.wait_for(start_entered.wait(), timeout=1)
    shutdown_task = asyncio.create_task(manager.shutdown())
    await asyncio.sleep(0)
    assert not shutdown_task.done()
    start_release.set()
    await asyncio.wait_for(shutdown_task, timeout=2)

    assert adapter.turns[0].interrupted is True
    row = manager.store.list(thread["id"])[0]
    assert row["state"] == "indeterminate"
    assert manager._dispatch_tasks == set()
    manager.store.close()


@pytest.mark.asyncio
async def test_dispatch_read_failure_enters_reconciliation_and_retries(tmp_path) -> None:
    manager, adapter = make_manager(tmp_path)
    thread = await manager.create_thread(None, "demo")
    manager.store.enqueue(thread["id"], "queued")
    original_read = adapter.read_thread
    calls = 0

    async def flaky_read(thread_id, *, archived=False):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("temporary transport failure")
        return await original_read(thread_id, archived=archived)

    adapter.read_thread = flaky_read
    await manager.dispatch_if_idle(thread["id"])
    assert manager._mode(thread["id"]) is Ownership.reconciling
    assert thread["id"] in manager._retry_handles
    await manager.dispatch_if_idle(thread["id"])
    assert adapter.turns
    adapter.turns[0].done.set()
    await asyncio.wait_for(manager._tasks[thread["id"]], timeout=1)
    await manager.shutdown()
    manager.store.close()


@pytest.mark.asyncio
async def test_start_timeout_makes_shutdown_bounded_and_queue_indeterminate(
    tmp_path,
) -> None:
    manager, adapter = make_manager(tmp_path)
    manager.start_timeout = 0.01
    manager.shutdown_timeout = 0.05
    thread = await manager.create_thread(None, "demo")
    never = asyncio.Event()
    started = asyncio.Event()

    async def stuck_start(thread_id, body):
        started.set()
        await never.wait()

    adapter.start_turn = stuck_start
    await manager.queue(thread["id"], "queued")
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(manager.shutdown(), timeout=1)

    row = manager.store.list(thread["id"])[0]
    assert row["state"] == "indeterminate"
    assert manager._dispatch_tasks == set()
    manager.store.close()
