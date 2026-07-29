from __future__ import annotations

import argparse
import shlex
from dataclasses import dataclass
from typing import Any

from .errors import ConsoleError
from .manager import ThreadManager


HELP = """Commands:
  thread list [--archived] [--all-local]
  thread create [--name NAME] [--cwd PATH]
  thread rename THREAD NAME
  thread archive THREAD
  thread delete THREAD
  thread restore THREAD
  thread status THREAD
  message send|steer|queue THREAD TEXT
  message interrupt THREAD
  queue list [THREAD]
  queue cancel|retry ID
  terminal command THREAD
"""


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise ConsoleError("invalid_command", message)


@dataclass(slots=True)
class ParsedCommand:
    group: str
    action: str
    args: argparse.Namespace


def parse_command(line: str) -> ParsedCommand:
    try:
        words = shlex.split(line)
    except ValueError as exc:
        raise ConsoleError("invalid_command", str(exc)) from exc
    if not words:
        raise ConsoleError("empty_command", "command cannot be empty")
    if words[0] == "help":
        return ParsedCommand("help", "show", argparse.Namespace())
    if len(words) < 2:
        raise ConsoleError("invalid_command", "expected GROUP ACTION")
    group, action, rest = words[0], words[1], words[2:]
    parser = Parser(add_help=False)
    if group == "thread" and action == "list":
        parser.add_argument("--archived", action="store_true")
        parser.add_argument("--all-local", action="store_true")
    elif group == "thread" and action == "create":
        parser.add_argument("--name")
        parser.add_argument("--cwd")
    elif group == "thread" and action == "rename":
        parser.add_argument("thread")
        parser.add_argument("name")
    elif group == "thread" and action in {"archive", "delete", "restore", "status"}:
        parser.add_argument("thread")
    elif group == "message" and action in {"send", "steer", "queue"}:
        parser.add_argument("thread")
        parser.add_argument("text", nargs="+")
    elif group == "message" and action == "interrupt":
        parser.add_argument("thread")
    elif group == "queue" and action == "list":
        parser.add_argument("thread", nargs="?")
    elif group == "queue" and action in {"cancel", "retry"}:
        parser.add_argument("id", type=int)
    elif group == "terminal" and action == "command":
        parser.add_argument("thread")
    else:
        raise ConsoleError("unknown_command", f"unknown command: {group} {action}")
    return ParsedCommand(group, action, parser.parse_args(rest))


async def execute_command(manager: ThreadManager, line: str) -> dict[str, Any]:
    command = parse_command(line)
    group, action, args = command.group, command.action, command.args
    if group == "help":
        return {"help": HELP}
    if group == "thread" and action == "list":
        return {
            "threads": await manager.list_threads(
                args.archived, include_all=args.all_local
            )
        }
    if group == "thread" and action == "create":
        return {"thread": await manager.create_thread(args.cwd, args.name)}
    if group == "queue" and action == "list":
        thread_id = (
            await manager.resolve_thread(args.thread) if args.thread else None
        )
        return {"queue": manager.store.list(thread_id)}
    if group == "queue" and action == "cancel":
        return {"item": await manager.cancel_queue(args.id)}
    if group == "queue" and action == "retry":
        return {"item": await manager.retry_queue(args.id)}

    if group == "thread" and action == "delete":
        thread_id = await manager.resolve_thread_any(args.thread)
        return await manager.delete(thread_id)

    archived = group == "thread" and action == "restore"
    thread_id = await manager.resolve_thread(args.thread, archived=archived)
    if group == "thread" and action == "rename":
        return {"thread": await manager.rename(thread_id, args.name)}
    if group == "thread" and action == "archive":
        return await manager.archive(thread_id)
    if group == "thread" and action == "restore":
        return {"thread": await manager.restore(thread_id)}
    if group == "thread" and action == "status":
        return await manager.status(thread_id)
    if group == "message" and action == "send":
        return await manager.send(thread_id, " ".join(args.text))
    if group == "message" and action == "steer":
        return await manager.steer(thread_id, " ".join(args.text))
    if group == "message" and action == "queue":
        return {"item": await manager.queue(thread_id, " ".join(args.text))}
    if group == "message" and action == "interrupt":
        return await manager.interrupt(thread_id)
    if group == "terminal" and action == "command":
        return {
            "thread_id": thread_id,
            "attach_url": f"/ws/terminal/{thread_id}",
            "note": "Use the Web UI Attach CLI button; this is not a shell command.",
        }
    raise ConsoleError("unknown_command", f"unsupported command: {line}")
