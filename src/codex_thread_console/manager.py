from __future__ import annotations

import asyncio
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Any

from .adapter import CodexAdapter, TurnLike
from .config import Settings
from .errors import ConflictError, ConsoleError
from .events import EventBroker
from .sequencer import MutationSequencer
from .store import QueueStore


class Ownership(str, Enum):
    idle = "idle"
    sdk_turn = "sdk_turn"
    reconciling = "reconciling"


class ThreadManager:
    def __init__(
        self,
        adapter: CodexAdapter,
        store: QueueStore,
        settings: Settings,
        events: EventBroker | None = None,
        *,
        start_timeout: float = 30.0,
        shutdown_timeout: float = 35.0,
    ) -> None:
        self.adapter = adapter
        self.store = store
        self.settings = settings
        self.events = events or EventBroker()
        self.start_timeout = start_timeout
        self.shutdown_timeout = shutdown_timeout
        self._locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._ownership: dict[str, Ownership] = {}
        self._pty_counts: defaultdict[str, int] = defaultdict(int)
        self._handles: dict[str, TurnLike] = {}
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._retry_handles: dict[str, asyncio.TimerHandle] = {}
        self._dispatch_tasks: set[asyncio.Task[None]] = set()
        self._start_lock = asyncio.Lock()
        self._sequencer = MutationSequencer()
        self._draining = False

    def _mode(self, thread_id: str) -> Ownership:
        return self._ownership.get(thread_id, Ownership.idle)

    def _publish(self, event_type: str, **payload: Any) -> None:
        self.events.publish({"type": event_type, **payload})

    async def startup(self) -> None:
        for thread_id in self.store.queued_thread_ids():
            self._trigger_dispatch(thread_id)

    async def shutdown(self) -> None:
        self._draining = True
        await self._sequencer.close()
        for retry in self._retry_handles.values():
            retry.cancel()
        self._retry_handles.clear()
        async def wait_for_start_barrier() -> None:
            async with self._start_lock:
                return

        try:
            await asyncio.wait_for(
                wait_for_start_barrier(), timeout=self.shutdown_timeout
            )
        except asyncio.TimeoutError:
            for task in tuple(self._dispatch_tasks):
                task.cancel()
        dispatches = list(self._dispatch_tasks)
        if dispatches:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*dispatches, return_exceptions=True), timeout=10
                )
            except asyncio.TimeoutError:
                for task in dispatches:
                    task.cancel()
                await asyncio.gather(*dispatches, return_exceptions=True)
        handles_by_thread = dict(self._handles)
        thread_ids = (
            set(self.store.managed_thread_ids())
            | set(self._pty_counts)
            | set(handles_by_thread)
        )
        interrupted: set[str] = set()
        if thread_ids:
            async def interrupt_authoritative(thread_id: str) -> tuple[str, bool]:
                try:
                    turn_id = await self.adapter.interrupt_active_turn(thread_id)
                    return thread_id, turn_id is not None
                except BaseException:
                    return thread_id, False

            try:
                results = await asyncio.wait_for(
                    asyncio.gather(
                        *(interrupt_authoritative(thread_id) for thread_id in thread_ids),
                        return_exceptions=True,
                    ),
                    timeout=10,
                )
                interrupted = {
                    thread_id
                    for result in results
                    if isinstance(result, tuple)
                    for thread_id, did_interrupt in [result]
                    if did_interrupt
                }
            except asyncio.TimeoutError:
                pass
        fallback_handles = [
            handle
            for thread_id, handle in handles_by_thread.items()
            if thread_id not in interrupted
        ]
        if fallback_handles:
            try:
                await asyncio.wait_for(
                    asyncio.gather(
                        *(handle.interrupt() for handle in fallback_handles),
                        return_exceptions=True,
                    ),
                    timeout=10,
                )
            except asyncio.TimeoutError:
                pass
        tasks = list(self._tasks.values())
        if tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*tasks, return_exceptions=True), timeout=10
                )
            except asyncio.TimeoutError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    def _ensure_serving(self) -> None:
        if self._draining:
            raise ConsoleError(
                "server_draining", "server is shutting down", status=503
            )

    async def _serialize_mutation(
        self,
        thread_id: str,
        action: str,
        operation: Any,
    ) -> Any:
        self._ensure_serving()
        sequence, result = await self._sequencer.submit(
            thread_id, action, operation
        )
        self._publish(
            "mutation_committed",
            thread_id=thread_id,
            action=action,
            mutation_sequence=sequence,
        )
        if isinstance(result, dict):
            result = {**result, "mutation_sequence": sequence}
        return result

    async def list_threads(
        self, archived: bool = False, *, include_all: bool = False
    ) -> list[dict[str, Any]]:
        """List console-owned threads unless the caller explicitly opts into history."""
        managed_rows = self.store.list_managed(archived)
        managed_by_id = {str(row["id"]): row for row in managed_rows}
        by_id: dict[str, dict[str, Any]] = {}

        if include_all:
            rows = await self.adapter.list_threads(archived)
            by_id.update({str(row["id"]): dict(row) for row in rows})

        for thread_id, managed in managed_by_id.items():
            try:
                row = await self.adapter.read_thread(thread_id, archived=archived)
            except Exception:
                row = {
                    "id": thread_id,
                    "name": managed["name"],
                    "preview": "",
                    "cwd": managed["cwd"],
                    "status": "draft" if managed["draft"] else "unavailable",
                    "archived": bool(managed["archived"]),
                    "created_at": managed["created_at"],
                    "updated_at": managed["updated_at"],
                    "model_provider": None,
                }
            row = dict(row)
            row["draft"] = bool(managed["draft"])
            row["source"] = "codex-thread-console"
            by_id[thread_id] = row

        rows = list(by_id.values())
        for row in rows:
            row.setdefault("draft", False)
            row.setdefault("source", "local-codex-history")
            row["ownership"] = self._mode(row["id"]).value
            row["terminal_attached"] = self._pty_counts[row["id"]] > 0
            row["queue_open"] = self.store.open_count(row["id"])
        return rows

    async def create_thread(self, cwd: str | None, name: str | None) -> dict[str, Any]:
        self._ensure_serving()
        try:
            safe_cwd = self.settings.validate_cwd(cwd or self.settings.workspace_root)
        except (OSError, ValueError) as exc:
            raise ConsoleError("invalid_cwd", str(exc)) from exc
        row = await self.adapter.create_thread(str(safe_cwd), name)
        self.store.register_thread(row["id"], row["cwd"], name)
        row["draft"] = True
        row["ownership"] = Ownership.idle.value
        row["terminal_attached"] = False
        row["queue_open"] = 0
        self._publish("thread_created", thread=row)
        return row

    async def resolve_thread(self, reference: str, *, archived: bool = False) -> str:
        rows = await self.list_threads(archived)
        exact = [row for row in rows if row["id"] == reference]
        if exact:
            return str(exact[0]["id"])
        names = [row for row in rows if row.get("name") == reference]
        if len(names) == 1:
            return str(names[0]["id"])
        if len(names) > 1:
            raise ConflictError(
                "ambiguous_thread", f"multiple threads are named {reference!r}"
            )
        raise ConsoleError(
            "thread_not_found", f"thread {reference!r} was not found", status=404
        )

    async def resolve_thread_any(self, reference: str) -> str:
        try:
            return await self.resolve_thread(reference)
        except ConsoleError as exc:
            if exc.code != "thread_not_found":
                raise
        return await self.resolve_thread(reference, archived=True)

    async def status(self, thread_id: str) -> dict[str, Any]:
        try:
            metadata = await self.adapter.read_thread(thread_id)
        except BaseException:
            managed = self.store.get_managed(thread_id)
            if managed is None:
                raise
            metadata = {
                "id": thread_id,
                "name": managed["name"],
                "cwd": managed["cwd"],
                "status": "draft" if managed["draft"] else "unavailable",
                "archived": bool(managed["archived"]),
                "draft": bool(managed["draft"]),
            }
        return {
            "thread": metadata,
            "ownership": self._mode(thread_id).value,
            "terminal_attached": self._pty_counts[thread_id] > 0,
            "queue": self.store.list(thread_id, open_only=True),
        }

    async def rename(self, thread_id: str, name: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "thread.rename",
            lambda: self._rename(thread_id, name),
        )

    async def _rename(self, thread_id: str, name: str) -> dict[str, Any]:
        self._ensure_serving()
        async with self._locks[thread_id]:
            self._require_not_archived(thread_id)
            self._require_idle(thread_id)
            await self._require_authoritative_idle(thread_id, "rename")
            row = await self.adapter.rename_thread(thread_id, name)
            self.store.update_managed(thread_id, name=name)
        self._publish("thread_renamed", thread=row)
        return row

    async def archive(self, thread_id: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "thread.archive",
            lambda: self._archive(thread_id),
        )

    async def _archive(self, thread_id: str) -> dict[str, Any]:
        self._ensure_serving()
        async with self._locks[thread_id]:
            self._require_idle(thread_id)
            await self._require_authoritative_idle(thread_id, "archive")
            if self.store.open_count(thread_id):
                raise ConflictError(
                    "queue_not_empty",
                    "cancel or resolve queued/indeterminate messages before archiving",
                )
            managed = self.store.get_managed(thread_id)
            if managed and managed["draft"]:
                self.store.update_managed(thread_id, archived=1)
            else:
                await self.adapter.archive_thread(thread_id)
                if managed:
                    self.store.update_managed(thread_id, archived=1)
        result = {"id": thread_id, "archived": True, "hard_deleted": False}
        self._publish("thread_archived", **result)
        return result

    async def delete(self, thread_id: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "thread.delete",
            lambda: self._delete(thread_id),
        )

    async def _delete(self, thread_id: str) -> dict[str, Any]:
        self._ensure_serving()
        async with self._locks[thread_id]:
            self._require_idle(thread_id)
            managed = self.store.get_managed(thread_id)
            if not managed or not managed["archived"]:
                await self._require_authoritative_idle(thread_id, "delete")
            await self.adapter.delete_thread(thread_id)
            self.store.delete_thread(thread_id)
        self._ownership.pop(thread_id, None)
        self._pty_counts.pop(thread_id, None)
        result = {"id": thread_id, "deleted": True}
        self._publish("thread_deleted", **result)
        return result

    async def restore(self, thread_id: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "thread.restore",
            lambda: self._restore(thread_id),
        )

    async def _restore(self, thread_id: str) -> dict[str, Any]:
        self._ensure_serving()
        async with self._locks[thread_id]:
            self._require_idle(thread_id)
            managed = self.store.get_managed(thread_id)
            if managed and managed["draft"]:
                self.store.update_managed(thread_id, archived=0)
                row = {
                    "id": thread_id,
                    "name": managed["name"],
                    "cwd": managed["cwd"],
                    "status": "draft",
                    "archived": False,
                    "draft": True,
                }
            else:
                row = await self.adapter.restore_thread(thread_id)
                if managed:
                    self.store.update_managed(thread_id, archived=0)
        self._publish("thread_restored", thread=row)
        return row

    def _require_idle(self, thread_id: str) -> None:
        mode = self._mode(thread_id)
        if mode is not Ownership.idle:
            raise ConflictError(
                "thread_busy", f"thread is owned by {mode.value}"
            )

    def _require_not_archived(self, thread_id: str) -> None:
        managed = self.store.get_managed(thread_id)
        if managed and managed["archived"]:
            raise ConflictError(
                "thread_archived", "restore the thread before mutating it"
            )

    async def _require_authoritative_idle(
        self, thread_id: str, action: str
    ) -> dict[str, Any]:
        metadata = await self.adapter.read_thread(thread_id)
        if metadata["status"] != "idle":
            raise ConflictError(
                "external_turn_active",
                f"cannot {action}: Codex thread is {metadata['status']}",
            )
        return metadata

    async def _ensure_sdk_quiescent(self, thread_id: str) -> dict[str, Any]:
        metadata = await self.adapter.read_thread(thread_id)
        if metadata["status"] != "idle":
            self._ownership[thread_id] = Ownership.reconciling
            self._schedule_dispatch_retry(thread_id)
            raise ConflictError(
                "external_turn_active",
                f"Codex thread is {metadata['status']}; waiting for confirmed idle",
            )
        return metadata

    def _schedule_dispatch_retry(self, thread_id: str) -> None:
        if self._draining or thread_id in self._retry_handles:
            return
        loop = asyncio.get_running_loop()

        def retry() -> None:
            self._retry_handles.pop(thread_id, None)
            if not self._draining:
                self._trigger_dispatch(thread_id)

        self._retry_handles[thread_id] = loop.call_later(2.0, retry)

    def _trigger_dispatch(self, thread_id: str) -> None:
        if self._draining:
            return
        task = asyncio.create_task(self.dispatch_if_idle(thread_id))
        self._dispatch_tasks.add(task)
        task.add_done_callback(self._dispatch_tasks.discard)

    async def send(self, thread_id: str, body: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "turn.start",
            lambda: self._send(thread_id, body),
        )

    async def _send(self, thread_id: str, body: str) -> dict[str, Any]:
        self._ensure_serving()
        if not body.strip():
            raise ConsoleError("empty_message", "message cannot be empty")
        async with self._locks[thread_id]:
            self._require_not_archived(thread_id)
            if self.store.open_count(thread_id):
                return self._enqueue_deferred_send(thread_id, body)
            if self._mode(thread_id) is not Ownership.idle:
                return self._enqueue_deferred_send(thread_id, body)
            metadata = await self.adapter.read_thread(thread_id)
            if metadata["status"] != "idle":
                self._ownership[thread_id] = Ownership.reconciling
                self._schedule_dispatch_retry(thread_id)
                return self._enqueue_deferred_send(thread_id, body)
            async with self._start_lock:
                if self._draining:
                    self._ensure_serving()
                try:
                    handle = await asyncio.wait_for(
                        self.adapter.start_turn(thread_id, body),
                        timeout=self.start_timeout,
                    )
                except BaseException as exc:
                    self._ownership[thread_id] = Ownership.reconciling
                    self._schedule_dispatch_retry(thread_id)
                    raise ConsoleError(
                        "turn_start_indeterminate",
                        f"Codex may have accepted the message; reconciliation required: {exc}",
                        status=502,
                    ) from exc
                if self._draining:
                    try:
                        await asyncio.wait_for(handle.interrupt(), timeout=5)
                    except BaseException:
                        pass
                    self._ownership[thread_id] = Ownership.reconciling
                    self._ensure_serving()
            self.store.update_managed(thread_id, draft=0)
            self._start_handle(thread_id, handle, queue_id=None)
        return {"thread_id": thread_id, "turn_id": handle.id, "state": "running"}

    def _enqueue_deferred_send(
        self, thread_id: str, body: str
    ) -> dict[str, Any]:
        item = self.store.enqueue(thread_id, body)
        self._publish("message_queued", item=item, reason="thread_busy")
        self._trigger_dispatch(thread_id)
        return {
            "thread_id": thread_id,
            "state": "queued",
            "queue_id": item["id"],
        }

    async def steer(self, thread_id: str, body: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "turn.steer",
            lambda: self._steer(thread_id, body),
        )

    async def _steer(self, thread_id: str, body: str) -> dict[str, Any]:
        self._ensure_serving()
        if not body.strip():
            raise ConsoleError("empty_message", "message cannot be empty")
        async with self._locks[thread_id]:
            if self._mode(thread_id) is Ownership.sdk_turn:
                handle = self._handles[thread_id]
                turn_id = handle.id
                response = await handle.steer(body)
            else:
                turn_id = await self.adapter.active_turn_id(thread_id)
                if turn_id is None:
                    raise ConflictError(
                        "no_active_turn",
                        "steer requires an active in-flight turn",
                    )
                response = await self.adapter.steer_turn(
                    thread_id, turn_id, body
                )
        self._publish("turn_steered", thread_id=thread_id, turn_id=turn_id)
        return {
            "thread_id": thread_id,
            "turn_id": turn_id,
            "accepted": True,
            "response": (
                response.model_dump(mode="json", by_alias=True)
                if hasattr(response, "model_dump")
                else response if isinstance(response, dict) else None
            ),
        }

    async def interrupt(self, thread_id: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "turn.interrupt",
            lambda: self._interrupt(thread_id),
        )

    async def _interrupt(self, thread_id: str) -> dict[str, Any]:
        async with self._locks[thread_id]:
            if self._mode(thread_id) is Ownership.sdk_turn:
                handle = self._handles[thread_id]
                turn_id = handle.id
                await handle.interrupt()
            else:
                turn_id = await self.adapter.active_turn_id(thread_id)
                if turn_id is None:
                    raise ConflictError(
                        "no_active_turn",
                        "interrupt requires an active in-flight turn",
                    )
                await self.adapter.interrupt_turn(thread_id, turn_id)
        return {"thread_id": thread_id, "turn_id": turn_id, "interrupted": True}

    async def queue(self, thread_id: str, body: str) -> dict[str, Any]:
        return await self._serialize_mutation(
            thread_id,
            "turn.queue",
            lambda: self._queue(thread_id, body),
        )

    async def _queue(self, thread_id: str, body: str) -> dict[str, Any]:
        self._ensure_serving()
        if not body.strip():
            raise ConsoleError("empty_message", "message cannot be empty")
        async with self._locks[thread_id]:
            self._require_not_archived(thread_id)
            await self.adapter.read_thread(thread_id)
            item = self.store.enqueue(thread_id, body)
        self._publish("message_queued", item=item)
        self._trigger_dispatch(thread_id)
        return item

    async def dispatch_if_idle(self, thread_id: str) -> None:
        if self._draining:
            return
        async with self._locks[thread_id]:
            if self._draining:
                return
            mode = self._mode(thread_id)
            if mode is Ownership.reconciling:
                try:
                    metadata = await self.adapter.read_thread(thread_id)
                except BaseException:
                    self._schedule_dispatch_retry(thread_id)
                    return
                if metadata["status"] != "idle":
                    self._schedule_dispatch_retry(thread_id)
                    return
                self._ownership[thread_id] = Ownership.idle
                mode = Ownership.idle
            if mode is not Ownership.idle:
                return
            try:
                await self._ensure_sdk_quiescent(thread_id)
            except ConflictError:
                return
            except BaseException:
                self._ownership[thread_id] = Ownership.reconciling
                self._schedule_dispatch_retry(thread_id)
                return
            item = self.store.claim_next(thread_id)
            if item is None:
                return
            async with self._start_lock:
                if self._draining:
                    self.store.finish(
                        int(item["id"]), "indeterminate", "server shutdown during dispatch"
                    )
                    return
                try:
                    handle = await asyncio.wait_for(
                        self.adapter.start_turn(thread_id, str(item["body"])),
                        timeout=self.start_timeout,
                    )
                    self.store.update_managed(thread_id, draft=0)
                    self.store.set_turn_id(int(item["id"]), handle.id)
                except BaseException as exc:
                    self.store.finish(int(item["id"]), "indeterminate", str(exc))
                    self._ownership[thread_id] = Ownership.reconciling
                    self._publish(
                        "queue_indeterminate",
                        item_id=item["id"],
                        thread_id=thread_id,
                        error=str(exc),
                    )
                    return
                if self._draining:
                    try:
                        await asyncio.wait_for(handle.interrupt(), timeout=5)
                    except BaseException:
                        pass
                    self.store.finish(
                        int(item["id"]),
                        "indeterminate",
                        "server shutdown after Codex accepted the turn",
                    )
                    self._ownership[thread_id] = Ownership.reconciling
                    return
            self._start_handle(thread_id, handle, queue_id=int(item["id"]))

    def _start_handle(
        self, thread_id: str, handle: TurnLike, queue_id: int | None
    ) -> None:
        self._ownership[thread_id] = Ownership.sdk_turn
        self._handles[thread_id] = handle
        task = asyncio.create_task(self._run_handle(thread_id, handle, queue_id))
        self._tasks[thread_id] = task
        self._publish(
            "turn_started",
            thread_id=thread_id,
            turn_id=handle.id,
            queue_id=queue_id,
        )

    async def _run_handle(
        self, thread_id: str, handle: TurnLike, queue_id: int | None
    ) -> None:
        error: str | None = None
        result: Any = None
        try:
            result = await handle.run()
            if queue_id is not None:
                raw_status = getattr(result, "status", None)
                status = getattr(raw_status, "value", raw_status)
                if status in {None, "completed"}:
                    self.store.finish(queue_id, "done")
                else:
                    error = f"turn ended with status {status}"
                    self.store.finish(queue_id, "failed", error)
        except asyncio.CancelledError:
            error = "turn supervision cancelled during shutdown"
            if queue_id is not None:
                self.store.finish(queue_id, "indeterminate", error)
            raise
        except BaseException as exc:
            error = str(exc)
            if queue_id is not None:
                self.store.finish(queue_id, "failed", error)
        finally:
            async with self._locks[thread_id]:
                if self._handles.get(thread_id) is handle:
                    self._handles.pop(thread_id, None)
                    self._tasks.pop(thread_id, None)
                    self._ownership[thread_id] = Ownership.idle
            self._publish(
                "turn_finished",
                thread_id=thread_id,
                turn_id=handle.id,
                error=error,
                final_response=getattr(result, "final_response", None),
            )
            if not self._draining:
                self._trigger_dispatch(thread_id)

    async def reserve_pty(self, thread_id: str) -> dict[str, Any]:
        self._ensure_serving()
        async with self._locks[thread_id]:
            self._require_not_archived(thread_id)
            metadata = await self.adapter.read_thread(thread_id)
            try:
                cwd = self.settings.validate_cwd(Path(metadata["cwd"]))
            except (OSError, ValueError) as exc:
                raise ConsoleError("invalid_thread_cwd", str(exc)) from exc
            if self.store.get_managed(thread_id):
                # An empty SDK-created thread can be resumed directly by the CLI.
                # From this point on it must be archived through Codex, not treated
                # as a local-only draft.
                self.store.update_managed(thread_id, draft=0)
            self._pty_counts[thread_id] += 1
            terminal_count = self._pty_counts[thread_id]
        self._publish(
            "pty_attached",
            thread_id=thread_id,
            terminal_count=terminal_count,
        )
        return {**metadata, "cwd": str(cwd)}

    async def begin_pty_stop(self, thread_id: str) -> None:
        async with self._locks[thread_id]:
            should_interrupt = (
                self._pty_counts[thread_id] <= 1
                and self._mode(thread_id) is not Ownership.sdk_turn
            )
            if not should_interrupt:
                return
            try:
                turn_id = await self.adapter.interrupt_active_turn(thread_id)
            except BaseException as exc:
                self._publish(
                    "pty_turn_interrupt_failed",
                    thread_id=thread_id,
                    error=str(exc),
                )
                return
        if turn_id is not None:
            self._publish(
                "pty_turn_interrupted",
                thread_id=thread_id,
                turn_id=turn_id,
            )

    async def release_pty(self, thread_id: str) -> None:
        async with self._locks[thread_id]:
            self._pty_counts[thread_id] = max(0, self._pty_counts[thread_id] - 1)
            terminal_count = self._pty_counts[thread_id]
        self._publish(
            "pty_detached",
            thread_id=thread_id,
            ownership=self._mode(thread_id).value,
            terminal_count=terminal_count,
        )

    async def cancel_queue(self, item_id: int) -> dict[str, Any]:
        existing = self.store.get(item_id)
        return await self._serialize_mutation(
            str(existing["thread_id"]),
            "queue.cancel",
            lambda: self._cancel_queue(item_id),
        )

    async def _cancel_queue(self, item_id: int) -> dict[str, Any]:
        item = self.store.cancel(item_id)
        self._publish("queue_cancelled", item=item)
        return item

    async def retry_queue(self, item_id: int) -> dict[str, Any]:
        existing = self.store.get(item_id)
        return await self._serialize_mutation(
            str(existing["thread_id"]),
            "queue.retry",
            lambda: self._retry_queue(item_id),
        )

    async def _retry_queue(self, item_id: int) -> dict[str, Any]:
        item = self.store.retry(item_id)
        self._publish("queue_retried", item=item)
        self._trigger_dispatch(str(item["thread_id"]))
        return item
