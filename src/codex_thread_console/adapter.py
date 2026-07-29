from __future__ import annotations

from typing import Any, Protocol

from openai_codex import (
    ApprovalMode,
    AsyncCodex,
    AsyncTurnHandle,
    CodexConfig,
    Sandbox,
)
from openai_codex.generated.v2_all import (
    Personality,
    ReasoningEffort,
    ReasoningSummary,
    ThreadDeleteResponse,
    ThreadSourceKind,
)

from .turns import TurnOptions


class TurnLike(Protocol):
    id: str

    async def run(self) -> Any: ...

    async def steer(self, input: str) -> Any: ...

    async def interrupt(self) -> Any: ...


def _status_type(metadata: Any) -> str:
    status = getattr(metadata, "status", None)
    root = getattr(status, "root", status)
    return str(getattr(root, "type", "unknown"))


def _thread_dto(metadata: Any, *, archived: bool) -> dict[str, Any]:
    cwd_model = getattr(metadata, "cwd", None)
    cwd = getattr(cwd_model, "root", cwd_model)
    return {
        "id": metadata.id,
        "name": getattr(metadata, "name", None),
        "preview": str(getattr(metadata, "preview", ""))[:240],
        "cwd": str(cwd),
        "status": _status_type(metadata),
        "archived": archived,
        "created_at": getattr(metadata, "created_at", None),
        "updated_at": getattr(metadata, "updated_at", None),
        "model_provider": getattr(metadata, "model_provider", None),
        "source": (
            metadata.source.model_dump(mode="json", by_alias=True)
            if hasattr(getattr(metadata, "source", None), "model_dump")
            else str(getattr(metadata, "source", "unknown"))
        ),
    }


def _turn_status(turn: Any) -> str:
    status = getattr(turn, "status", None)
    return str(getattr(status, "value", status))


class CodexAdapter:
    """Narrow adapter around the public async SDK surface."""

    def __init__(
        self,
        client: AsyncCodex | None = None,
        *,
        config: CodexConfig | None = None,
    ) -> None:
        self._client = client or AsyncCodex(config=config)
        self._owns_client = client is None

    async def start(self) -> None:
        if self._owns_client:
            await self._client.__aenter__()

    async def close(self) -> None:
        if self._owns_client:
            try:
                await self._client.__aexit__(None, None, None)
            except BrokenPipeError:
                # The stdio proxy may observe app-server transport closure
                # before the SDK closes its stdin. Shutdown is already complete
                # from the SDK transport's perspective in that case.
                pass

    async def create_thread(self, cwd: str, name: str | None) -> dict[str, Any]:
        thread = await self._client.thread_start(
            cwd=cwd,
            approval_mode=ApprovalMode.deny_all,
            sandbox=Sandbox.read_only,
        )
        if name:
            await thread.set_name(name)
        return await self.read_thread(thread.id)

    async def list_threads(self, archived: bool) -> list[dict[str, Any]]:
        cursor: str | None = None
        result: list[dict[str, Any]] = []
        sources = list(ThreadSourceKind)
        while True:
            page = await self._client.thread_list(
                archived=archived,
                cursor=cursor,
                limit=100,
                source_kinds=sources,
            )
            result.extend(_thread_dto(item, archived=archived) for item in page.data)
            cursor = page.next_cursor
            if cursor is None:
                return result

    async def read_thread(self, thread_id: str, *, archived: bool = False) -> dict[str, Any]:
        thread = await self._client.thread_resume(thread_id)
        response = await thread.read()
        return _thread_dto(response.thread, archived=archived)

    async def rename_thread(self, thread_id: str, name: str) -> dict[str, Any]:
        thread = await self._client.thread_resume(thread_id)
        await thread.set_name(name)
        return await self.read_thread(thread_id)

    async def archive_thread(self, thread_id: str) -> None:
        await self._client.thread_archive(thread_id)

    async def delete_thread(self, thread_id: str) -> None:
        # openai-codex 0.144.4 ships the generated thread/delete schema but
        # does not yet expose a flat AsyncCodex.thread_delete convenience
        # method. Keep this single pinned-version escape hatch in the adapter.
        await self._client._ensure_initialized()
        await self._client._client.request(
            "thread/delete",
            {"threadId": thread_id},
            response_model=ThreadDeleteResponse,
        )

    async def restore_thread(self, thread_id: str) -> dict[str, Any]:
        thread = await self._client.thread_unarchive(thread_id)
        response = await thread.read()
        return _thread_dto(response.thread, archived=False)

    async def start_turn(
        self,
        thread_id: str,
        body: str,
        options: TurnOptions | None = None,
    ) -> TurnLike:
        options = options or {}
        thread = await self._client.thread_resume(
            thread_id,
            approval_mode=ApprovalMode.deny_all,
            sandbox=Sandbox.read_only,
        )
        return await thread.turn(
            body,
            approval_mode=ApprovalMode.deny_all,
            cwd=options.get("cwd"),
            effort=(
                ReasoningEffort(options["effort"])
                if "effort" in options
                else None
            ),
            model=options.get("model"),
            output_schema=options.get("output_schema"),
            personality=(
                Personality(options["personality"])
                if "personality" in options
                else None
            ),
            sandbox=Sandbox.read_only,
            service_tier=options.get("service_tier"),
            summary=(
                ReasoningSummary.model_validate(options["summary"])
                if "summary" in options
                else None
            ),
        )

    async def active_turn_id(self, thread_id: str) -> str | None:
        thread = await self._client.thread_resume(thread_id)
        response = await thread.read(include_turns=True)
        for turn in reversed(response.thread.turns):
            if _turn_status(turn) == "inProgress":
                return str(turn.id)
        return None

    async def steer_turn(
        self, thread_id: str, turn_id: str, body: str
    ) -> Any:
        handle = AsyncTurnHandle(self._client, thread_id, turn_id)
        return await handle.steer(body)

    async def interrupt_turn(self, thread_id: str, turn_id: str) -> Any:
        handle = AsyncTurnHandle(self._client, thread_id, turn_id)
        return await handle.interrupt()

    async def interrupt_active_turn(self, thread_id: str) -> str | None:
        turn_id = await self.active_turn_id(thread_id)
        if turn_id is None:
            return None
        await self.interrupt_turn(thread_id, turn_id)
        return turn_id
