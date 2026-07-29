from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient

import codex_thread_console.app as app_module
from codex_thread_console.app import create_app
from codex_thread_console.config import Settings

from .fakes import FakeAdapter


def test_cookie_and_header_auth(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    settings = Settings(
        workspace_root=workspace,
        data_dir=data,
        host="127.0.0.1",
        port=8765,
        session_token="secret",
    )
    app = create_app(settings=settings, adapter=FakeAdapter(workspace))
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        assert client.get("/api/health").status_code == 401
        root = client.get("/")
        assert root.status_code == 200
        assert client.get("/api/health").status_code == 200

        other = TestClient(app, base_url="http://127.0.0.1:8765")
        response = other.post(
            "/api/command",
            json={"command": "help"},
            headers={"X-Console-Token": "secret"},
        )
        assert response.status_code == 200
        assert "thread list" in response.json()["help"]


def test_forbidden_browser_origin(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    settings = Settings(workspace, data, port=8765, session_token="secret")
    app = create_app(settings=settings, adapter=FakeAdapter(workspace))
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        client.get("/")
        response = client.post(
            "/api/command",
            json={"command": "help"},
            headers={"Origin": "https://evil.example"},
        )
        assert response.status_code == 403


def test_thread_history_requires_explicit_opt_in(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    settings = Settings(workspace, data, port=8765, session_token="secret")
    adapter = FakeAdapter(workspace)
    adapter.rows["external-thread"] = {
        "id": "external-thread",
        "name": "external",
        "preview": "",
        "cwd": str(workspace),
        "status": "idle",
        "archived": False,
    }
    app = create_app(settings=settings, adapter=adapter)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        client.get("/")
        assert client.get("/api/threads").json()["threads"] == []
        rows = client.get("/api/threads?include_all=true").json()["threads"]
        assert [row["id"] for row in rows] == ["external-thread"]


def test_archive_and_delete_have_distinct_semantics(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    settings = Settings(workspace, data, port=8765, session_token="secret")
    adapter = FakeAdapter(workspace)
    app = create_app(settings=settings, adapter=adapter)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        client.get("/")
        created = client.post(
            "/api/threads", json={"name": "disposable"}
        ).json()["thread"]
        thread_id = created["id"]

        archived = client.post(f"/api/threads/{thread_id}/archive")
        assert archived.status_code == 200
        archived_rows = client.get("/api/threads?archived=true").json()["threads"]
        assert [row["id"] for row in archived_rows] == [thread_id]

        restored = client.post(f"/api/threads/{thread_id}/restore")
        assert restored.status_code == 200
        deleted = client.delete(f"/api/threads/{thread_id}")
        assert deleted.status_code == 200
        assert deleted.json()["deleted"] is True
        assert thread_id not in adapter.rows


def test_second_server_does_not_overwrite_live_token(tmp_path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    first_settings = Settings(
        workspace, data, port=8765, session_token="first-token"
    )
    second_settings = Settings(
        workspace, data, port=8765, session_token="second-token"
    )
    first = create_app(settings=first_settings, adapter=FakeAdapter(workspace))
    second = create_app(settings=second_settings, adapter=FakeAdapter(workspace))

    with TestClient(first, base_url="http://127.0.0.1:8765"):
        assert first_settings.token_path.read_text() == "first-token"
        with pytest.raises(RuntimeError, match="another console server"):
            with TestClient(second, base_url="http://127.0.0.1:8765"):
                pass
        assert first_settings.token_path.read_text() == "first-token"


def test_terminal_websocket_signals_ready_and_acknowledges_input(
    tmp_path, monkeypatch
) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    data = tmp_path / "data"
    settings = Settings(workspace, data, port=8765, session_token="secret")
    adapter = FakeAdapter(workspace)
    adapter.rows["terminal-thread"] = {
        "id": "terminal-thread",
        "name": "terminal",
        "preview": "",
        "cwd": str(workspace),
        "status": "idle",
        "archived": False,
    }

    class FakePtySession:
        instances = []

        def __init__(self, thread_id, cwd, executable=None, remote_url=None):
            self.thread_id = thread_id
            self.cwd = cwd
            self.executable = executable
            self.remote_url = remote_url
            self.started = None
            self.inputs = []
            self.closed = asyncio.Event()
            self.__class__.instances.append(self)

        def start(self, cols=120, rows=36):
            self.started = (cols, rows)

        async def write_all(self, data):
            self.inputs.append(data)

        def resize(self, cols, rows):
            self.started = (cols, rows)

        async def read(self):
            await self.closed.wait()
            return b""

        async def stop(self):
            self.closed.set()

    monkeypatch.setattr(app_module, "PtySession", FakePtySession)
    app = create_app(settings=settings, adapter=adapter)
    with TestClient(app, base_url="http://127.0.0.1:8765") as client:
        client.get("/")
        with client.websocket_connect(
            "/ws/terminal/terminal-thread?cols=91&rows=31",
            headers={
                "Origin": "http://127.0.0.1:8765",
                "Host": "127.0.0.1:8765",
                "Cookie": "codex_console_session=secret",
            },
        ) as websocket:
            assert websocket.receive_json() == {
                "type": "terminal_ready",
                "thread_id": "terminal-thread",
            }
            websocket.send_json({"type": "input", "id": "message-1", "data": "hi"})
            assert websocket.receive_json() == {
                "type": "input_ack",
                "id": "message-1",
            }

    session = FakePtySession.instances[0]
    assert session.started == (91, 31)
    assert session.inputs == ["hi"]
