from __future__ import annotations

import pytest

from codex_thread_console.commands import parse_command
from codex_thread_console.errors import ConsoleError


def test_parses_quoted_message() -> None:
    parsed = parse_command('message queue "thread one" "keep this together"')
    assert parsed.group == "message"
    assert parsed.action == "queue"
    assert parsed.args.thread == "thread one"
    assert parsed.args.text == ["keep this together"]


def test_rejects_unknown_command() -> None:
    with pytest.raises(ConsoleError, match="unknown command"):
        parse_command("shell exec whoami")


def test_thread_list_all_local_is_explicit() -> None:
    parsed = parse_command("thread list --archived --all-local")
    assert parsed.args.archived is True
    assert parsed.args.all_local is True
