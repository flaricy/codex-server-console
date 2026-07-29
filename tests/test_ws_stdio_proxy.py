from __future__ import annotations

import json

from codex_thread_console.ws_stdio_proxy import compact_notification


def test_compact_notification_keeps_bounded_lifecycle_fields() -> None:
    raw = json.dumps(
        {
            "method": "turn/started",
            "params": {
                "threadId": "thread-1",
                "turn": {
                    "id": "turn-1",
                    "status": "inProgress",
                    "items": [{"output": "must not be copied"}],
                },
                "unrelated": "ignored",
            },
        }
    )

    compact = compact_notification(raw)

    assert compact is not None
    assert json.loads(compact) == {
        "method": "turn/started",
        "params": {
            "threadId": "thread-1",
            "turn": {"id": "turn-1", "status": "inProgress"},
        },
    }


def test_compact_notification_ignores_responses_and_high_volume_deltas() -> None:
    assert compact_notification('{"id":"1","result":{}}') is None
    assert (
        compact_notification(
            '{"method":"item/agentMessage/delta","params":{"delta":"hello"}}'
        )
        is None
    )
