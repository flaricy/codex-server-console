from __future__ import annotations

import argparse
import http.client
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

from .config import default_data_dir

try:
    # input() only enables terminal line editing after this module is imported.
    # Both GNU readline and macOS libedit provide cursor movement, Home/End,
    # deletion, and in-process command history.
    import readline as _readline  # noqa: F401
except ImportError:  # pragma: no cover - uncommon on supported Unix platforms
    _readline = None


def _default_token_path() -> Path:
    data = Path(
        os.environ.get(
            "CODEX_CONSOLE_DATA_DIR",
            default_data_dir(),
        )
    ).expanduser().resolve()
    return data / "session-token"


class ConsoleConnectionError(RuntimeError):
    pass


def _short(value: object, limit: int) -> str:
    text = "" if value is None else str(value).replace("\n", " ")
    if len(text) <= limit:
        return text
    return f"{text[: max(0, limit - 1)]}…"


def _table(headers: tuple[str, ...], rows: list[tuple[object, ...]]) -> str:
    rendered = [[_short(value, 48) for value in row] for row in rows]
    widths = [
        max(
            len(headers[index]),
            *(len(row[index]) for row in rendered),
        )
        for index in range(len(headers))
    ]
    header = "  ".join(
        value.ljust(widths[index]) for index, value in enumerate(headers)
    )
    separator = "  ".join("-" * width for width in widths)
    body = [
        "  ".join(
            value.ljust(widths[index]) for index, value in enumerate(row)
        ).rstrip()
        for row in rendered
    ]
    return "\n".join([header, separator, *body])


def render_result(result: dict[str, object]) -> str:
    error = result.get("error")
    if isinstance(error, dict):
        code = error.get("code", "error")
        return f"{code}: {error.get('message', 'unknown error')}"
    help_text = result.get("help")
    if isinstance(help_text, str):
        return help_text.rstrip()
    threads = result.get("threads")
    if isinstance(threads, list):
        if not threads:
            return "No threads."
        return _table(
            ("STATUS", "SCOPE", "QUEUE", "NAME", "THREAD"),
            [
                (
                    row.get("status", "unknown"),
                    (
                        "inspect"
                        if row.get("controllable") is False
                        else "control"
                    ),
                    row.get("queue_open", 0),
                    row.get("name") or row.get("preview") or "",
                    row.get("id", ""),
                )
                for row in threads
                if isinstance(row, dict)
            ],
        )
    queue = result.get("queue")
    if isinstance(queue, list):
        if not queue:
            return "Queue is empty."
        return _table(
            ("ID", "STATE", "THREAD", "MESSAGE"),
            [
                (
                    row.get("id", ""),
                    row.get("state", ""),
                    row.get("thread_id", ""),
                    row.get("body", ""),
                )
                for row in queue
                if isinstance(row, dict)
            ],
        )
    return json.dumps(result, indent=2, ensure_ascii=False)


class ConsoleClient:
    """One authenticated, reusable HTTP connection to the console server."""

    def __init__(self, url: str, token: str, *, timeout: float = 30) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "http"
            or not parsed.hostname
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("--url must be a plain http://host[:port] URL")
        self._base_path = parsed.path.rstrip("/")
        self._token = token
        self._connection = http.client.HTTPConnection(
            parsed.hostname,
            parsed.port or 80,
            timeout=timeout,
        )

    def close(self) -> None:
        self._connection.close()

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, object] | None = None,
    ) -> dict[str, object]:
        encoded = None if body is None else json.dumps(body).encode("utf-8")
        headers = {"X-Console-Token": self._token}
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        try:
            self._connection.request(
                method,
                f"{self._base_path}{path}",
                body=encoded,
                headers=headers,
            )
            response = self._connection.getresponse()
            payload = response.read()
        except (OSError, http.client.HTTPException) as exc:
            self.close()
            raise ConsoleConnectionError(str(exc)) from exc

        try:
            result = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ConsoleConnectionError(
                f"server returned an invalid response (HTTP {response.status})"
            ) from exc
        if not isinstance(result, dict):
            raise ConsoleConnectionError(
                f"server returned an invalid response (HTTP {response.status})"
            )
        return result

    def connect(self) -> dict[str, object]:
        """Open TCP now and verify that the authenticated server is ready."""
        result = self._request("GET", "/api/health")
        if result.get("ok") is not True:
            error = result.get("error")
            raise ConsoleConnectionError(
                f"server health check failed: {error or result}"
            )
        return result

    def call(self, command: str) -> dict[str, object]:
        return self._request("POST", "/api/command", {"command": command})


def call(url: str, token: str, command: str) -> dict[str, object]:
    """Compatibility helper for callers that issue one command."""
    client = ConsoleClient(url, token)
    try:
        client.connect()
        return client.call(command)
    finally:
        client.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Codex Thread Console shell")
    parser.add_argument("--url", default="http://127.0.0.1:8765")
    parser.add_argument("--token-file", type=Path, default=_default_token_path())
    parser.add_argument(
        "--json",
        action="store_true",
        help="print machine-readable JSON instead of human-oriented output",
    )
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    try:
        token = args.token_file.read_text(encoding="utf-8").strip()
    except OSError as exc:
        parser.error(f"cannot read server token: {exc}")
    try:
        client = ConsoleClient(args.url, token)
        health = client.connect()
    except (ConsoleConnectionError, ValueError) as exc:
        parser.exit(
            2,
            f"codex-thread-console: cannot connect to {args.url}: {exc}\n"
            "Start codex-thread-console-server first.\n",
        )

    try:
        if args.command:
            result = client.call(" ".join(args.command))
            print(
                json.dumps(result, indent=2, ensure_ascii=False)
                if args.json
                else render_result(result)
            )
            return

        version = health.get("app_server_version", "unknown")
        print(
            f"Codex Thread Console. Connected to app-server {version}. "
            "Type help or exit."
        )
        while True:
            try:
                line = input("codex-console> ")
            except (EOFError, KeyboardInterrupt):
                print()
                return
            if line.strip() in {"exit", "quit"}:
                return
            if not line.strip():
                continue
            try:
                result = client.call(line)
            except ConsoleConnectionError as exc:
                print(f"Connection lost: {exc}", file=sys.stderr)
                return
            print(
                json.dumps(result, indent=2, ensure_ascii=False)
                if args.json
                else render_result(result)
            )
    finally:
        client.close()


if __name__ == "__main__":
    main()
