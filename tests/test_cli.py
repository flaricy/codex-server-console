from __future__ import annotations

import json

import pytest

import codex_thread_console.cli as cli_module
from codex_thread_console.cli import (
    ConsoleClient,
    ConsoleConnectionError,
    render_result,
)


class _Response:
    status = 200

    def __init__(self, payload: dict[str, object]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:
        return self._body


class _Connection:
    instances: list["_Connection"] = []

    def __init__(self, host: str, port: int, timeout: float) -> None:
        self.host = host
        self.port = port
        self.timeout = timeout
        self.requests: list[tuple[str, str, bytes | None, dict[str, str]]] = []
        self.closed = False
        self.__class__.instances.append(self)

    def request(
        self,
        method: str,
        path: str,
        body: bytes | None,
        headers: dict[str, str],
    ) -> None:
        self.requests.append((method, path, body, headers))

    def getresponse(self) -> _Response:
        method, path, body, _headers = self.requests[-1]
        if method == "GET":
            return _Response({"ok": True, "app_server_version": "test"})
        assert path == "/api/command"
        command = json.loads(body or b"{}")["command"]
        return _Response({"command": command})

    def close(self) -> None:
        self.closed = True


def test_client_connects_immediately_and_reuses_tcp_connection(monkeypatch) -> None:
    _Connection.instances = []
    monkeypatch.setattr(cli_module.http.client, "HTTPConnection", _Connection)
    client = ConsoleClient("http://127.0.0.1:8765", "secret")
    try:
        assert client.connect()["ok"] is True
        assert client.call("thread list") == {"command": "thread list"}
    finally:
        client.close()

    assert len(_Connection.instances) == 1
    connection = _Connection.instances[0]
    assert [request[1] for request in connection.requests] == [
        "/api/health",
        "/api/command",
    ]
    assert all(
        request[3]["X-Console-Token"] == "secret"
        for request in connection.requests
    )
    assert connection.closed is True


def test_connect_fails_before_entering_repl_when_server_is_absent(
    monkeypatch,
) -> None:
    class RefusedConnection(_Connection):
        def request(self, *args, **kwargs) -> None:
            raise ConnectionRefusedError("Connection refused")

    monkeypatch.setattr(
        cli_module.http.client, "HTTPConnection", RefusedConnection
    )
    client = ConsoleClient("http://127.0.0.1:8765", "secret", timeout=0.2)
    with pytest.raises(ConsoleConnectionError):
        client.connect()


def test_human_thread_list_is_compact() -> None:
    rendered = render_result(
        {
            "threads": [
                {
                    "id": "thread-1",
                    "name": "workflow",
                    "status": "idle",
                    "queue_open": 0,
                    "created_here": True,
                },
                {
                    "id": "thread-2",
                    "preview": "existing session",
                    "status": "active",
                    "queue_open": 2,
                    "created_here": False,
                },
            ]
        }
    )

    assert "STATUS" in rendered
    assert "workflow" in rendered
    assert "existing session" in rendered
