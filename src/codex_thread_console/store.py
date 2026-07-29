from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any

from .errors import ConflictError, NotFoundError


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


class QueueStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = RLock()
        self._connection = sqlite3.connect(path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._initialize()
        path.chmod(0o600)

    def _initialize(self) -> None:
        with self._connection:
            self._connection.execute("PRAGMA journal_mode=WAL")
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS queued_messages(
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id TEXT NOT NULL,
                    body TEXT NOT NULL,
                    options_json TEXT NOT NULL DEFAULT '{}',
                    state TEXT NOT NULL CHECK(state IN (
                      'queued','running','indeterminate','done','failed','cancelled'
                    )),
                    turn_id TEXT,
                    created_at TEXT NOT NULL,
                    claimed_at TEXT,
                    finished_at TEXT,
                    error TEXT
                )
                """
            )
            columns = {
                str(row["name"])
                for row in self._connection.execute(
                    "PRAGMA table_info(queued_messages)"
                ).fetchall()
            }
            if "options_json" not in columns:
                self._connection.execute(
                    "ALTER TABLE queued_messages "
                    "ADD COLUMN options_json TEXT NOT NULL DEFAULT '{}'"
                )
            self._connection.execute(
                """
                CREATE TABLE IF NOT EXISTS managed_threads(
                    id TEXT PRIMARY KEY,
                    cwd TEXT NOT NULL,
                    name TEXT,
                    archived INTEGER NOT NULL DEFAULT 0,
                    draft INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            self._connection.execute(
                "UPDATE queued_messages SET state='indeterminate', "
                "error=COALESCE(error, 'server restarted while message was running') "
                "WHERE state='running'"
            )

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        result = dict(row)
        raw_options = result.pop("options_json", "{}")
        try:
            options = json.loads(raw_options or "{}")
        except (TypeError, json.JSONDecodeError):
            options = {}
        result["options"] = options if isinstance(options, dict) else {}
        return result

    def enqueue(
        self,
        thread_id: str,
        body: str,
        options: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        encoded_options = json.dumps(
            options or {},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        with self._lock, self._connection:
            cursor = self._connection.execute(
                "INSERT INTO queued_messages"
                "(thread_id,body,options_json,state,created_at) "
                "VALUES(?,?,?,'queued',?)",
                (thread_id, body, encoded_options, _now()),
            )
            row = self._connection.execute(
                "SELECT * FROM queued_messages WHERE id=?", (cursor.lastrowid,)
            ).fetchone()
        return self._dict(row) or {}

    def claim_next(self, thread_id: str) -> dict[str, Any] | None:
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                row = self._connection.execute(
                    "SELECT id,state FROM queued_messages "
                    "WHERE thread_id=? "
                    "AND state IN ('queued','running','indeterminate') "
                    "ORDER BY id LIMIT 1",
                    (thread_id,),
                ).fetchone()
                if row is None or row["state"] != "queued":
                    self._connection.commit()
                    return None
                claimed = self._connection.execute(
                    "UPDATE queued_messages SET state='running',claimed_at=? "
                    "WHERE id=? AND state='queued' RETURNING *",
                    (_now(), row["id"]),
                ).fetchone()
                self._connection.commit()
                return self._dict(claimed)
            except BaseException:
                self._connection.rollback()
                raise

    def set_turn_id(self, item_id: int, turn_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE queued_messages SET turn_id=? WHERE id=? AND state='running'",
                (turn_id, item_id),
            )

    def finish(self, item_id: int, state: str, error: str | None = None) -> None:
        if state not in {"done", "failed", "indeterminate"}:
            raise ValueError(f"invalid terminal queue state: {state}")
        with self._lock, self._connection:
            self._connection.execute(
                "UPDATE queued_messages SET state=?,finished_at=?,error=? "
                "WHERE id=? AND state='running'",
                (state, _now(), error, item_id),
            )

    def list(
        self, thread_id: str | None = None, *, open_only: bool = False
    ) -> list[dict[str, Any]]:
        sql = "SELECT * FROM queued_messages"
        clauses: list[str] = []
        args: list[Any] = []
        if thread_id:
            clauses.append("thread_id=?")
            args.append(thread_id)
        if open_only:
            clauses.append("state IN ('queued','running','indeterminate')")
        if clauses:
            sql += " WHERE " + " AND ".join(clauses)
        sql += " ORDER BY id"
        with self._lock:
            rows = self._connection.execute(sql, args).fetchall()
        return [self._dict(row) or {} for row in rows]

    def get(self, item_id: int) -> dict[str, Any]:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM queued_messages WHERE id=?", (item_id,)
            ).fetchone()
        if row is None:
            raise NotFoundError("queue_not_found", f"queue item {item_id} not found")
        return self._dict(row) or {}

    def open_count(self, thread_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS n FROM queued_messages WHERE thread_id=? "
                "AND state IN ('queued','running','indeterminate')",
                (thread_id,),
            ).fetchone()
        return int(row["n"])

    def cancel(self, item_id: int) -> dict[str, Any]:
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT * FROM queued_messages WHERE id=?", (item_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("queue_not_found", f"queue item {item_id} not found")
            if row["state"] not in {"queued", "indeterminate"}:
                raise ConflictError(
                    "queue_not_cancelable",
                    f"queue item {item_id} is {row['state']}; interrupt a running turn",
                )
            updated = self._connection.execute(
                "UPDATE queued_messages SET state='cancelled',finished_at=? "
                "WHERE id=? RETURNING *",
                (_now(), item_id),
            ).fetchone()
        return self._dict(updated) or {}

    def retry(self, item_id: int) -> dict[str, Any]:
        with self._lock, self._connection:
            row = self._connection.execute(
                "SELECT * FROM queued_messages WHERE id=?", (item_id,)
            ).fetchone()
            if row is None:
                raise NotFoundError("queue_not_found", f"queue item {item_id} not found")
            if row["state"] != "indeterminate":
                raise ConflictError(
                    "queue_not_retryable",
                    f"queue item {item_id} is {row['state']}, not indeterminate",
                )
            updated = self._connection.execute(
                "UPDATE queued_messages SET state='queued',claimed_at=NULL,"
                "finished_at=NULL,error=NULL,turn_id=NULL WHERE id=? RETURNING *",
                (item_id,),
            ).fetchone()
        return self._dict(updated) or {}

    def queued_thread_ids(self) -> list[str]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT DISTINCT thread_id FROM queued_messages WHERE state='queued'"
            ).fetchall()
        return [str(row["thread_id"]) for row in rows]

    def register_thread(self, thread_id: str, cwd: str, name: str | None) -> None:
        now = _now()
        with self._lock, self._connection:
            self._connection.execute(
                "INSERT OR REPLACE INTO managed_threads"
                "(id,cwd,name,archived,draft,created_at,updated_at) "
                "VALUES(?,?,?,0,1,?,?)",
                (thread_id, cwd, name, now, now),
            )

    def get_managed(self, thread_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM managed_threads WHERE id=?", (thread_id,)
            ).fetchone()
        return self._dict(row)

    def list_managed(self, archived: bool) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM managed_threads WHERE archived=? ORDER BY updated_at DESC",
                (int(archived),),
            ).fetchall()
        return [dict(row) for row in rows]

    def update_managed(self, thread_id: str, **fields: Any) -> None:
        allowed = {"name", "archived", "draft", "cwd"}
        updates = {key: value for key, value in fields.items() if key in allowed}
        if not updates:
            return
        updates["updated_at"] = _now()
        assignments = ",".join(f"{key}=?" for key in updates)
        with self._lock, self._connection:
            self._connection.execute(
                f"UPDATE managed_threads SET {assignments} WHERE id=?",
                (*updates.values(), thread_id),
            )

    def managed_thread_ids(self) -> list[str]:
        with self._lock:
            rows = self._connection.execute(
                "SELECT id FROM managed_threads"
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def delete_thread(self, thread_id: str) -> None:
        with self._lock, self._connection:
            self._connection.execute(
                "DELETE FROM queued_messages WHERE thread_id=?", (thread_id,)
            )
            self._connection.execute(
                "DELETE FROM managed_threads WHERE id=?", (thread_id,)
            )

    def close(self) -> None:
        with self._lock:
            self._connection.close()
