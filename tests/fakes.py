from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from typing import Any


class FakeTurn:
    def __init__(self, turn_id: str) -> None:
        self.id = turn_id
        self.done = asyncio.Event()
        self.steers: list[str] = []
        self.interrupted = False
        self.failure: Exception | None = None
        self.result_status = "completed"

    async def run(self) -> Any:
        await self.done.wait()
        if self.failure:
            raise self.failure
        return SimpleNamespace(
            final_response=f"finished {self.id}",
            status=SimpleNamespace(value=self.result_status),
        )

    async def steer(self, input: str) -> dict[str, object]:
        self.steers.append(input)
        return {"accepted": True}

    async def interrupt(self) -> dict[str, object]:
        self.interrupted = True
        self.result_status = "interrupted"
        self.done.set()
        return {"interrupted": True}


class FakeAdapter:
    def __init__(self, cwd: Path) -> None:
        self.cwd = cwd
        self.rows: dict[str, dict[str, Any]] = {}
        self.archived: set[str] = set()
        self.turns: list[FakeTurn] = []
        self.turns_by_id: dict[str, FakeTurn] = {}
        self.active_turn_ids: dict[str, str] = {}
        self.started = False
        self.closed = False

    async def start(self) -> None:
        self.started = True

    async def close(self) -> None:
        self.closed = True

    async def create_thread(self, cwd: str, name: str | None) -> dict[str, Any]:
        thread_id = f"thread-{len(self.rows) + 1}"
        row = {
            "id": thread_id,
            "name": name,
            "preview": "",
            "cwd": cwd,
            "status": "idle",
            "archived": False,
        }
        self.rows[thread_id] = row
        return dict(row)

    async def list_threads(self, archived: bool) -> list[dict[str, Any]]:
        return [
            {**row, "archived": archived}
            for thread_id, row in self.rows.items()
            if (thread_id in self.archived) is archived
        ]

    async def read_thread(
        self, thread_id: str, *, archived: bool = False
    ) -> dict[str, Any]:
        if thread_id not in self.rows:
            raise RuntimeError("not found")
        return {**self.rows[thread_id], "archived": archived}

    async def rename_thread(self, thread_id: str, name: str) -> dict[str, Any]:
        self.rows[thread_id]["name"] = name
        return dict(self.rows[thread_id])

    async def archive_thread(self, thread_id: str) -> None:
        self.archived.add(thread_id)

    async def delete_thread(self, thread_id: str) -> None:
        self.rows.pop(thread_id)
        self.archived.discard(thread_id)
        self.active_turn_ids.pop(thread_id, None)

    async def restore_thread(self, thread_id: str) -> dict[str, Any]:
        self.archived.remove(thread_id)
        return dict(self.rows[thread_id])

    async def start_turn(self, thread_id: str, body: str) -> FakeTurn:
        turn = FakeTurn(f"turn-{len(self.turns) + 1}")
        self.turns.append(turn)
        self.turns_by_id[turn.id] = turn
        self.active_turn_ids[thread_id] = turn.id
        return turn

    async def active_turn_id(self, thread_id: str) -> str | None:
        return self.active_turn_ids.get(thread_id)

    async def steer_turn(
        self, thread_id: str, turn_id: str, body: str
    ) -> dict[str, object]:
        return await self.turns_by_id[turn_id].steer(body)

    async def interrupt_turn(
        self, thread_id: str, turn_id: str
    ) -> dict[str, object]:
        result = await self.turns_by_id[turn_id].interrupt()
        self.active_turn_ids.pop(thread_id, None)
        if thread_id in self.rows:
            self.rows[thread_id]["status"] = "idle"
        return result

    async def interrupt_active_turn(self, thread_id: str) -> str | None:
        turn_id = self.active_turn_ids.get(thread_id)
        if turn_id is None:
            return None
        await self.interrupt_turn(thread_id, turn_id)
        return turn_id

    def add_external_turn(self, thread_id: str) -> FakeTurn:
        turn = FakeTurn(f"external-{len(self.turns) + 1}")
        self.turns.append(turn)
        self.turns_by_id[turn.id] = turn
        self.active_turn_ids[thread_id] = turn.id
        self.rows[thread_id]["status"] = "active"
        return turn
