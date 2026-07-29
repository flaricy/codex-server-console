from __future__ import annotations

import asyncio
import fcntl
import hmac
import json
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import (
    Cookie,
    Depends,
    FastAPI,
    Request,
    Response,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from openai_codex import __version__ as sdk_version

from .adapter import CodexAdapter
from .app_server_runtime import AppServerRuntime
from .commands import execute_command
from .config import Settings
from .errors import ConsoleError
from .events import EventBroker
from .manager import ThreadManager
from .pty_bridge import PtySession
from .store import QueueStore


class ThreadCreate(BaseModel):
    cwd: str | None = None
    name: str | None = None


class ThreadRename(BaseModel):
    name: str


class MessageBody(BaseModel):
    text: str


class CommandBody(BaseModel):
    command: str


def _allowed_origins(settings: Settings) -> set[str]:
    return {
        settings.origin,
        f"http://localhost:{settings.port}",
        f"http://127.0.0.1:{settings.port}",
    }


def _authorized(value: str | None, settings: Settings) -> bool:
    return value is not None and hmac.compare_digest(value, settings.session_token)


def create_app(
    *,
    settings: Settings | None = None,
    adapter: CodexAdapter | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    frontend_dist = Path(__file__).resolve().parents[2] / "frontend" / "dist"
    lock_file: Any = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal lock_file
        if settings.host not in {"127.0.0.1", "localhost", "::1"}:
            raise RuntimeError(
                "non-loopback binding is disabled for this local-only experiment"
            )
        settings.data_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
        settings.data_dir.chmod(0o700)
        lock_file = settings.lock_path.open("a+")
        try:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            lock_file.close()
            raise RuntimeError("another console server owns this data directory") from exc
        # Do not invalidate the live server's client token when a second
        # process fails the single-instance lock.
        settings.write_token_file()

        runtime: AppServerRuntime | None = None
        codex = adapter
        store = QueueStore(settings.database_path)
        events = EventBroker()
        manager: ThreadManager | None = None
        try:
            if codex is None:
                runtime = AppServerRuntime(
                    settings.data_dir, codex_bin=settings.codex_bin
                )
                await runtime.start()
                codex = CodexAdapter(config=runtime.sdk_config())
            manager = ThreadManager(codex, store, settings, events)
            app.state.settings = settings
            app.state.manager = manager
            app.state.app_server_runtime = runtime
            await codex.start()
            await manager.startup()
            yield
        finally:
            cleanup_errors: list[BaseException] = []
            if manager is not None:
                try:
                    await manager.shutdown()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if codex is not None:
                try:
                    await codex.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            if runtime is not None:
                try:
                    await runtime.close()
                except BaseException as exc:
                    cleanup_errors.append(exc)
            try:
                store.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
            try:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                lock_file.close()
            except BaseException as exc:
                cleanup_errors.append(exc)
            if cleanup_errors:
                raise cleanup_errors[0]

    app = FastAPI(title="Codex Thread Console", lifespan=lifespan)

    @app.exception_handler(ConsoleError)
    async def console_error_handler(_request: Request, exc: ConsoleError):
        return JSONResponse(exc.as_dict(), status_code=exc.status)

    def get_manager(request: Request) -> ThreadManager:
        return request.app.state.manager

    async def require_auth(
        request: Request,
        codex_console_session: str | None = Cookie(default=None),
    ) -> None:
        token = request.headers.get("x-console-token") or codex_console_session
        if not _authorized(token, settings):
            raise ConsoleError("unauthorized", "invalid console session", status=401)
        origin = request.headers.get("origin")
        if origin and origin not in _allowed_origins(settings):
            raise ConsoleError("forbidden_origin", "origin is not allowed", status=403)

    @app.get("/", include_in_schema=False)
    async def index() -> Response:
        if frontend_dist.joinpath("index.html").exists():
            response: Response = FileResponse(frontend_dist / "index.html")
        else:
            response = HTMLResponse(
                "<h1>Frontend not built</h1><p>Run <code>npm run build</code> "
                "inside <code>frontend</code>.</p>",
                status_code=503,
            )
        response.set_cookie(
            "codex_console_session",
            settings.session_token,
            httponly=True,
            samesite="strict",
            secure=False,
            path="/",
        )
        return response

    if frontend_dist.exists():
        app.mount("/assets", StaticFiles(directory=frontend_dist / "assets"), name="assets")

    @app.get("/api/health", dependencies=[Depends(require_auth)])
    async def health(request: Request) -> dict[str, object]:
        runtime: AppServerRuntime | None = getattr(
            request.app.state, "app_server_runtime", None
        )
        return {
            "ok": True,
            "sdk": f"openai-codex {sdk_version}",
            "app_server": "shared_loopback",
            "app_server_version": (
                runtime.codex_version if runtime is not None else "test-adapter"
            ),
            "app_server_binary": (
                str(runtime.codex_bin) if runtime is not None else None
            ),
            "app_server_binary_source": (
                runtime.codex_bin_source if runtime is not None else "test-adapter"
            ),
            "mutation_ordering": "fifo_per_thread",
            "background_policy": "deny_all/read_only",
        }

    @app.get("/api/threads", dependencies=[Depends(require_auth)])
    async def list_threads(
        archived: bool = False,
        include_all: bool = False,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {
            "threads": await manager.list_threads(
                archived, include_all=include_all
            )
        }

    @app.post("/api/threads", dependencies=[Depends(require_auth)])
    async def create_thread(
        body: ThreadCreate,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"thread": await manager.create_thread(body.cwd, body.name)}

    @app.patch("/api/threads/{thread_id}", dependencies=[Depends(require_auth)])
    async def rename_thread(
        thread_id: str,
        body: ThreadRename,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"thread": await manager.rename(thread_id, body.name)}

    @app.post(
        "/api/threads/{thread_id}/archive", dependencies=[Depends(require_auth)]
    )
    async def archive_thread(
        thread_id: str,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await manager.archive(thread_id)

    @app.delete("/api/threads/{thread_id}", dependencies=[Depends(require_auth)])
    async def delete_thread(
        thread_id: str,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await manager.delete(thread_id)

    @app.post(
        "/api/threads/{thread_id}/restore", dependencies=[Depends(require_auth)]
    )
    async def restore_thread(
        thread_id: str,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"thread": await manager.restore(thread_id)}

    @app.get(
        "/api/threads/{thread_id}/status", dependencies=[Depends(require_auth)]
    )
    async def thread_status(
        thread_id: str,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await manager.status(thread_id)

    @app.post(
        "/api/threads/{thread_id}/messages/send",
        dependencies=[Depends(require_auth)],
    )
    async def send_message(
        thread_id: str,
        body: MessageBody,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await manager.send(thread_id, body.text)

    @app.post(
        "/api/threads/{thread_id}/messages/steer",
        dependencies=[Depends(require_auth)],
    )
    async def steer_message(
        thread_id: str,
        body: MessageBody,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await manager.steer(thread_id, body.text)

    @app.post(
        "/api/threads/{thread_id}/messages/queue",
        dependencies=[Depends(require_auth)],
    )
    async def queue_message(
        thread_id: str,
        body: MessageBody,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"item": await manager.queue(thread_id, body.text)}

    @app.post(
        "/api/threads/{thread_id}/interrupt", dependencies=[Depends(require_auth)]
    )
    async def interrupt(
        thread_id: str,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await manager.interrupt(thread_id)

    @app.get("/api/queue", dependencies=[Depends(require_auth)])
    async def list_queue(
        thread_id: str | None = None,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"queue": manager.store.list(thread_id)}

    @app.delete("/api/queue/{item_id}", dependencies=[Depends(require_auth)])
    async def cancel_queue(
        item_id: int,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"item": await manager.cancel_queue(item_id)}

    @app.post("/api/queue/{item_id}/retry", dependencies=[Depends(require_auth)])
    async def retry_queue(
        item_id: int,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return {"item": await manager.retry_queue(item_id)}

    @app.post("/api/command", dependencies=[Depends(require_auth)])
    async def command(
        body: CommandBody,
        manager: ThreadManager = Depends(get_manager),
    ) -> dict[str, object]:
        return await execute_command(manager, body.command)

    async def authenticate_websocket(websocket: WebSocket) -> bool:
        token = websocket.cookies.get("codex_console_session")
        origin = websocket.headers.get("origin")
        host = websocket.headers.get("host")
        expected_hosts = {
            f"{settings.host}:{settings.port}",
            f"localhost:{settings.port}",
            f"127.0.0.1:{settings.port}",
        }
        if (
            not _authorized(token, settings)
            or origin not in _allowed_origins(settings)
            or host not in expected_hosts
        ):
            await websocket.close(code=4403)
            return False
        return True

    @app.websocket("/ws/events")
    async def event_socket(websocket: WebSocket) -> None:
        if not await authenticate_websocket(websocket):
            return
        await websocket.accept()
        manager: ThreadManager = websocket.app.state.manager
        async with manager.events.subscribe() as queue:
            try:
                async def send_events() -> None:
                    while True:
                        try:
                            event = await asyncio.wait_for(queue.get(), timeout=20)
                        except asyncio.TimeoutError:
                            # Keep otherwise-idle event sockets alive through
                            # browser and proxy idle timeouts. The frontend
                            # deliberately ignores this control message.
                            await websocket.send_json({"type": "heartbeat"})
                        else:
                            await websocket.send_json(event)

                async def watch_disconnect() -> None:
                    while True:
                        message = await websocket.receive()
                        if message["type"] == "websocket.disconnect":
                            return

                tasks = {
                    asyncio.create_task(send_events()),
                    asyncio.create_task(watch_disconnect()),
                }
                done, pending = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                await asyncio.gather(*done, *pending, return_exceptions=True)
            except WebSocketDisconnect:
                return

    @app.websocket("/ws/terminal/{thread_id}")
    async def terminal_socket(websocket: WebSocket, thread_id: str) -> None:
        if not await authenticate_websocket(websocket):
            return
        await websocket.accept()
        manager: ThreadManager = websocket.app.state.manager
        session: PtySession | None = None
        reserved = False
        try:
            metadata = await manager.reserve_pty(thread_id)
            reserved = True
            runtime: AppServerRuntime | None = getattr(
                websocket.app.state, "app_server_runtime", None
            )
            session = PtySession(
                thread_id,
                Path(str(metadata["cwd"])),
                executable=runtime.codex_bin if runtime is not None else None,
                remote_url=runtime.remote_url if runtime is not None else None,
            )
            try:
                initial_cols = int(websocket.query_params.get("cols", "120"))
                initial_rows = int(websocket.query_params.get("rows", "36"))
            except ValueError:
                initial_cols, initial_rows = 120, 36
            # Codex uses an inline TUI. It must know the browser's real grid
            # before its first paint; resizing a 120-column initial frame later
            # leaves stale box-drawing cells mixed with the new layout.
            session.start(cols=initial_cols, rows=initial_rows)
            send_lock = asyncio.Lock()
            await websocket.send_json(
                {"type": "terminal_ready", "thread_id": thread_id}
            )

            async def output_loop() -> None:
                assert session is not None
                while True:
                    data = await session.read()
                    if not data:
                        return
                    async with send_lock:
                        await websocket.send_bytes(data)

            async def input_loop() -> None:
                assert session is not None
                while True:
                    raw = await websocket.receive_text()
                    message = json.loads(raw)
                    if message.get("type") == "input":
                        await session.write_all(str(message.get("data", "")))
                        message_id = message.get("id")
                        if isinstance(message_id, str):
                            async with send_lock:
                                await websocket.send_json(
                                    {"type": "input_ack", "id": message_id}
                                )
                    elif message.get("type") == "resize":
                        session.resize(
                            int(message.get("cols", 120)),
                            int(message.get("rows", 36)),
                        )

            tasks = {
                asyncio.create_task(output_loop()),
                asyncio.create_task(input_loop()),
            }
            done, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in pending:
                task.cancel()
            await asyncio.gather(*done, *pending, return_exceptions=True)
        except (WebSocketDisconnect, RuntimeError, OSError, json.JSONDecodeError):
            pass
        except ConsoleError as exc:
            await websocket.send_json(exc.as_dict())
        finally:
            if reserved:
                await manager.begin_pty_stop(thread_id)
            if session is not None:
                await session.stop()
            if reserved:
                await manager.release_pty(thread_id)
            try:
                await websocket.close()
            except RuntimeError:
                pass

    return app


def main() -> None:
    settings = Settings.from_env()
    uvicorn.run(
        create_app(settings=settings),
        host=settings.host,
        port=settings.port,
        workers=1,
    )


if __name__ == "__main__":
    main()
