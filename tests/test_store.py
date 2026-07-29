from __future__ import annotations

from codex_thread_console.store import QueueStore


def test_claim_is_fifo_and_restart_is_indeterminate(tmp_path) -> None:
    path = tmp_path / "queue.sqlite3"
    store = QueueStore(path)
    first = store.enqueue("t1", "first")
    store.enqueue("t1", "second")
    claimed = store.claim_next("t1")
    assert claimed is not None
    assert claimed["id"] == first["id"]
    store.close()

    restarted = QueueStore(path)
    states = {row["id"]: row["state"] for row in restarted.list("t1")}
    assert states[first["id"]] == "indeterminate"
    assert list(states.values()).count("queued") == 1
    restarted.close()


def test_cancel_and_explicit_retry(tmp_path) -> None:
    store = QueueStore(tmp_path / "queue.sqlite3")
    item = store.enqueue("t1", "message")
    claimed = store.claim_next("t1")
    assert claimed is not None
    store.finish(item["id"], "indeterminate", "unknown")
    retried = store.retry(item["id"])
    assert retried["state"] == "queued"
    cancelled = store.cancel(item["id"])
    assert cancelled["state"] == "cancelled"
    store.close()


def test_indeterminate_item_blocks_later_fifo_items(tmp_path) -> None:
    store = QueueStore(tmp_path / "queue.sqlite3")
    first = store.enqueue("t1", "first")
    assert store.claim_next("t1") is not None
    store.finish(first["id"], "indeterminate", "unknown outcome")
    second = store.enqueue("t1", "second")

    assert store.claim_next("t1") is None
    store.cancel(first["id"])
    claimed = store.claim_next("t1")
    assert claimed is not None
    assert claimed["id"] == second["id"]
    store.close()


def test_delete_thread_removes_managed_row_and_queue(tmp_path) -> None:
    store = QueueStore(tmp_path / "queue.sqlite3")
    store.register_thread("t1", str(tmp_path), "demo")
    store.enqueue("t1", "message")

    store.delete_thread("t1")

    assert store.get_managed("t1") is None
    assert store.list("t1") == []
    assert "t1" not in store.managed_thread_ids()
    store.close()
