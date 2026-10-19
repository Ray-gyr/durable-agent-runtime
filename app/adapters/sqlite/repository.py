"""SQLite implementation of the Repository port.

Each public method is one transaction: it validates in a fixed order, raises a
stable error code, and either commits all of its writes or none of them.

Concurrency model: every write starts with `BEGIN IMMEDIATE`, which takes the
database write lock up front. That serializes writers across threads and
processes, so per-agent mutations and buffer claims never interleave. The lock
is global rather than per key, which trades write concurrency for simplicity.
Readers use WAL snapshots and never block writers.
"""

from __future__ import annotations

import asyncio
import json
import re
import sqlite3
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, TypeVar
from uuid import uuid4

from ...clock import Clock, SystemClock, format_timestamp
from ...errors import DomainError
from ...ports import Row
from .migrate import apply_migrations


T = TypeVar("T")

MAX_FLUSH_ATTEMPTS = 5
MAX_BACKOFF_SECONDS = 300
MAX_ERROR_LENGTH = 2000
BUFFER_STATUSES = ("pending", "processing", "flushed", "failed")

JSON_COLUMNS: dict[str, set[str]] = {
    "agent_registry": {"llm_config"},
    "tasks": {"input_payload", "output_payload"},
    "agent_runtime_log": {"message"},
    "buffer_events": {"payload"},
}

# Tables that backup/restore may touch, in foreign-key order.
RESTORABLE_TABLES = ("agent_registry", "agent_skills", "tasks", "agent_runtime_log", "agent_runtime_current")
ORDER_BY_PATTERN = re.compile(r"^[a-z_]+(, [a-z_]+)*$")


def shift(now: str, seconds: float) -> str:
    return format_timestamp(datetime.fromisoformat(now) + timedelta(seconds=seconds))


def encode_json(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def decode_row(table: str, row: sqlite3.Row | None) -> Row | None:
    if row is None:
        return None
    result = dict(row)
    for column in JSON_COLUMNS.get(table, set()):
        if result.get(column) is not None:
            result[column] = json.loads(result[column])
    return result


class SqliteRepository:
    def __init__(self, path: str | Path, clock: Clock | None = None, busy_timeout_seconds: float = 10.0) -> None:
        self.path = str(path)
        self.clock = clock or SystemClock()
        self.busy_timeout_seconds = busy_timeout_seconds
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        conn = self._connect()
        try:
            apply_migrations(conn)
        finally:
            conn.close()

    # ------------------------------------------------------------------ plumbing

    def _connect(self) -> sqlite3.Connection:
        # isolation_level=None disables the sqlite3 module's implicit transactions;
        # every transaction boundary below is explicit.
        conn = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_seconds,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA synchronous = NORMAL")
        return conn

    @contextmanager
    def _transaction(self, write: bool) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE" if write else "BEGIN")
            yield conn
            conn.execute("COMMIT")
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def _run_sync(self, operation: Callable[[sqlite3.Connection, str], T], write: bool) -> T:
        try:
            with self._transaction(write) as conn:
                # One timestamp per transaction, taken after the lock is held,
                # so commit order and timestamp order agree.
                return operation(conn, format_timestamp(self.clock.now()))
        except DomainError:
            raise
        except sqlite3.IntegrityError as exc:
            raise DomainError(409, "DATABASE_ERROR", "Database request failed") from exc
        except sqlite3.Error as exc:
            raise DomainError(502, "DATABASE_ERROR", "Database request failed") from exc

    async def _run(self, operation: Callable[[sqlite3.Connection, str], T], *, write: bool) -> T:
        return await asyncio.to_thread(self._run_sync, operation, write)

    async def close(self) -> None:
        return None

    # -------------------------------------------------------------------- agents

    @staticmethod
    def _fetch_agent(conn: sqlite3.Connection, aid: str) -> Row:
        agent = decode_row("agent_registry", conn.execute("SELECT * FROM agent_registry WHERE aid = ?", (aid,)).fetchone())
        if agent is None:
            raise DomainError.of("NOT_FOUND")
        skills = conn.execute("SELECT skill FROM agent_skills WHERE aid = ? ORDER BY rowid", (aid,)).fetchall()
        return {**agent, "skills": [row["skill"] for row in skills]}

    async def create_agent(self, model_provider: str, skills: list[str], llm_config: dict[str, Any]) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            provider = (model_provider or "").strip()
            if not provider:
                raise DomainError.of("INVALID_INPUT")
            aid = str(uuid4())
            conn.execute(
                "INSERT INTO agent_registry (aid, model_provider, llm_config, status, created_at, updated_at)"
                " VALUES (?, ?, ?, 'active', ?, ?)",
                (aid, provider, encode_json(llm_config or {}), now, now),
            )
            for skill in skills or []:
                if skill.strip():
                    conn.execute("INSERT OR IGNORE INTO agent_skills (aid, skill) VALUES (?, ?)", (aid, skill.strip()))
            conn.execute(
                "INSERT INTO agent_runtime_current (aid, state, updated_at) VALUES (?, 'idle', ?)",
                (aid, now),
            )
            return self._fetch_agent(conn, aid)

        return await self._run(operation, write=True)

    async def get_agent(self, aid: str) -> Row:
        return await self._run(lambda conn, _: self._fetch_agent(conn, aid), write=False)

    async def list_agents(self, status: str | None, skills: list[str]) -> list[Row]:
        def operation(conn: sqlite3.Connection, _: str) -> list[Row]:
            sql, params = "SELECT * FROM agent_registry", []
            if status:
                sql, params = sql + " WHERE status = ?", [status]
            agents = [decode_row("agent_registry", row) for row in conn.execute(sql + " ORDER BY created_at, aid", params)]
            skill_map: dict[str, list[str]] = {}
            for row in conn.execute("SELECT aid, skill FROM agent_skills ORDER BY rowid"):
                skill_map.setdefault(row["aid"], []).append(row["skill"])
            return [
                {**agent, "skills": skill_map.get(agent["aid"], [])}
                for agent in agents
                if agent is not None and all(required in skill_map.get(agent["aid"], []) for required in skills)
            ]

        return await self._run(operation, write=False)

    async def modify_agent(self, aid: str, llm_config: dict[str, Any]) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            current = conn.execute("SELECT state FROM agent_runtime_current WHERE aid = ?", (aid,)).fetchone()
            if current is None:
                raise DomainError.of("NOT_FOUND")
            if current["state"] == "running":
                raise DomainError.of("AGENT_BUSY")
            updated = conn.execute(
                "UPDATE agent_registry SET llm_config = ?, updated_at = ? WHERE aid = ? AND status = 'active'",
                (encode_json(llm_config or {}), now, aid),
            )
            if updated.rowcount == 0:
                exists = conn.execute("SELECT 1 FROM agent_registry WHERE aid = ?", (aid,)).fetchone()
                raise DomainError.of("AGENT_DISABLED" if exists else "NOT_FOUND")
            return self._fetch_agent(conn, aid)

        return await self._run(operation, write=True)

    async def disable_agent(self, aid: str) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            current = conn.execute("SELECT state FROM agent_runtime_current WHERE aid = ?", (aid,)).fetchone()
            if current is None:
                raise DomainError.of("NOT_FOUND")
            if current["state"] == "running":
                raise DomainError.of("AGENT_BUSY")
            updated = conn.execute(
                "UPDATE agent_registry SET status = 'disabled', disabled_at = ?, updated_at = ?"
                " WHERE aid = ? AND status = 'active'",
                (now, now, aid),
            )
            if updated.rowcount == 0:
                # Disabling an already-disabled agent is idempotent.
                return self._fetch_agent(conn, aid)
            conn.execute(
                "UPDATE agent_runtime_current SET state = 'disabled', task_id = NULL, updated_at = ? WHERE aid = ?",
                (now, aid),
            )
            conn.execute(
                "INSERT INTO agent_runtime_log (state_id, aid, state, timestamp) VALUES (?, ?, 'disabled', ?)",
                (str(uuid4()), aid, now),
            )
            return self._fetch_agent(conn, aid)

        return await self._run(operation, write=True)

    # --------------------------------------------------------------------- tasks

    @staticmethod
    def _fetch_task(conn: sqlite3.Connection, task_id: str) -> Row:
        task = decode_row("tasks", conn.execute("SELECT * FROM tasks WHERE task_id = ?", (task_id,)).fetchone())
        if task is None:
            raise DomainError.of("TASK_NOT_FOUND")
        return task

    async def create_task(self, aid: str, input_payload: dict[str, Any]) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            agent = conn.execute("SELECT status FROM agent_registry WHERE aid = ?", (aid,)).fetchone()
            if agent is None:
                raise DomainError.of("NOT_FOUND")
            if agent["status"] == "disabled":
                raise DomainError.of("AGENT_DISABLED")
            conn.execute(
                "INSERT OR IGNORE INTO agent_runtime_current (aid, state, updated_at) VALUES (?, 'idle', ?)",
                (aid, now),
            )
            # Insert first so agent_runtime_current.task_id has a valid foreign key.
            # If the claim below fails, the transaction rolls this insert back.
            task_id = str(uuid4())
            conn.execute(
                "INSERT INTO tasks (task_id, aid, input_payload, status, created_at) VALUES (?, ?, ?, 'pending', ?)",
                (task_id, aid, encode_json(input_payload), now),
            )
            # The conditional update is the atomic busy check: only one caller can
            # move this agent from idle to running.
            claimed = conn.execute(
                "UPDATE agent_runtime_current SET task_id = ?, state = 'running', updated_at = ?"
                " WHERE aid = ? AND state = 'idle'",
                (task_id, now, aid),
            )
            if claimed.rowcount == 0:
                raise DomainError.of("AGENT_BUSY")
            conn.execute(
                "INSERT INTO agent_runtime_log (state_id, aid, task_id, state, timestamp) VALUES (?, ?, ?, 'running', ?)",
                (str(uuid4()), aid, task_id, now),
            )
            return self._fetch_task(conn, task_id)

        return await self._run(operation, write=True)

    async def get_task(self, task_id: str) -> Row:
        return await self._run(lambda conn, _: self._fetch_task(conn, task_id), write=False)

    async def list_tasks(self, aid: str | None, status: str | None, since: datetime | None) -> list[Row]:
        def operation(conn: sqlite3.Connection, _: str) -> list[Row]:
            clauses: list[str] = []
            params: list[Any] = []
            if aid:
                clauses.append("aid = ?")
                params.append(aid)
            if status:
                clauses.append("status = ?")
                params.append(status)
            if since:
                clauses.append("created_at >= ?")
                params.append(format_timestamp(since))
            where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
            rows = conn.execute(f"SELECT * FROM tasks{where} ORDER BY created_at DESC, rowid DESC", params)
            return [row for row in (decode_row("tasks", item) for item in rows) if row is not None]

        return await self._run(operation, write=False)

    async def record_task_result(self, task_id: str, output_payload: Any, status: str) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            if status not in ("completed", "failed"):
                raise DomainError.of("INVALID_TERMINAL_STATUS")
            task = conn.execute("SELECT aid FROM tasks WHERE task_id = ?", (task_id,)).fetchone()
            if task is None:
                raise DomainError.of("TASK_NOT_FOUND")
            # A terminal task cannot be written twice; duplicate callbacks fail atomically.
            updated = conn.execute(
                "UPDATE tasks SET output_payload = ?, status = ?, completed_at = ?"
                " WHERE task_id = ? AND status IN ('pending', 'running')",
                (encode_json(output_payload), status, now, task_id),
            )
            if updated.rowcount == 0:
                raise DomainError.of("TASK_ALREADY_TERMINAL")
            conn.execute(
                "UPDATE agent_runtime_current SET task_id = NULL, state = 'idle', updated_at = ?"
                " WHERE aid = ? AND task_id = ? AND state = 'running'",
                (now, task["aid"], task_id),
            )
            conn.execute(
                "INSERT INTO agent_runtime_log (state_id, aid, task_id, state, timestamp) VALUES (?, ?, ?, 'idle', ?)",
                (str(uuid4()), task["aid"], task_id, now),
            )
            return self._fetch_task(conn, task_id)

        return await self._run(operation, write=True)

    # ------------------------------------------------------------- runtime state

    async def get_agent_state(self, aid: str) -> Row:
        def operation(conn: sqlite3.Connection, _: str) -> Row:
            row = conn.execute(
                "SELECT state_id, aid, task_id, state, timestamp, message FROM agent_runtime_log"
                " WHERE aid = ? ORDER BY timestamp DESC, rowid DESC LIMIT 1",
                (aid,),
            ).fetchone()
            state = decode_row("agent_runtime_log", row)
            if state is None:
                raise DomainError.of("NOT_FOUND")
            return state

        return await self._run(operation, write=False)

    async def record_agent_state(
        self,
        aid: str,
        task_id: str | None,
        state: str,
        timestamp: datetime | None,
        message: Any,
    ) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            if state not in ("idle", "running", "offline", "disabled"):
                raise DomainError.of("INVALID_STATE")
            agent = conn.execute("SELECT status FROM agent_registry WHERE aid = ?", (aid,)).fetchone()
            if agent is None:
                raise DomainError.of("NOT_FOUND")
            if state == "disabled" and agent["status"] != "disabled":
                raise DomainError.of("AGENT_NOT_DISABLED")
            if state == "running" and task_id is None:
                raise DomainError.of("TASK_REQUIRED")
            if state != "running" and task_id is not None:
                raise DomainError.of("TASK_NOT_ALLOWED")
            if state == "running" and not conn.execute(
                "SELECT 1 FROM tasks WHERE task_id = ? AND aid = ? AND status IN ('pending', 'running')",
                (task_id, aid),
            ).fetchone():
                raise DomainError.of("TASK_NOT_FOUND")

            at = format_timestamp(timestamp) if timestamp else now
            current = conn.execute("SELECT task_id FROM agent_runtime_current WHERE aid = ?", (aid,)).fetchone()
            if current is None:
                conn.execute(
                    "INSERT INTO agent_runtime_current (aid, task_id, state, updated_at) VALUES (?, ?, ?, ?)",
                    (aid, task_id, state, at),
                )
            elif current["task_id"] is None or current["task_id"] == task_id:
                conn.execute(
                    "UPDATE agent_runtime_current SET task_id = ?, state = ?, updated_at = ? WHERE aid = ?",
                    (task_id, state, at, aid),
                )
            else:
                # The agent is attached to a different task.
                raise DomainError.of("AGENT_BUSY")

            state_id = str(uuid4())
            conn.execute(
                "INSERT INTO agent_runtime_log (state_id, aid, task_id, state, timestamp, message)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (state_id, aid, task_id, state, at, encode_json(message)),
            )
            row = conn.execute("SELECT * FROM agent_runtime_log WHERE state_id = ?", (state_id,)).fetchone()
            return decode_row("agent_runtime_log", row) or {}

        return await self._run(operation, write=True)

    # -------------------------------------------------------------------- buffer

    async def enqueue_buffer_event(self, event_type: str, payload: Any, event_timestamp: datetime | None) -> Row:
        def operation(conn: sqlite3.Connection, now: str) -> Row:
            clean_type = (event_type or "").strip()
            if not clean_type or payload is None:
                raise DomainError.of("INVALID_INPUT")
            inserted = conn.execute(
                "INSERT INTO buffer_events (event_id, event_type, payload, event_timestamp, available_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (
                    str(uuid4()),
                    clean_type,
                    encode_json(payload),
                    format_timestamp(event_timestamp) if event_timestamp else now,
                    now,
                    now,
                ),
            )
            row = conn.execute("SELECT * FROM buffer_events WHERE seq = ?", (inserted.lastrowid,)).fetchone()
            return decode_row("buffer_events", row) or {}

        return await self._run(operation, write=True)

    async def get_buffer_status(self) -> dict[str, int]:
        def operation(conn: sqlite3.Connection, _: str) -> dict[str, int]:
            counts = dict.fromkeys(BUFFER_STATUSES, 0)
            for row in conn.execute("SELECT status, count(*) AS event_count FROM buffer_events GROUP BY status"):
                counts[row["status"]] = int(row["event_count"])
            return counts

        return await self._run(operation, write=False)

    @staticmethod
    def _recover_expired_leases(conn: sqlite3.Connection, now: str) -> int:
        recovered = conn.execute(
            """
            UPDATE buffer_events
               SET status = CASE WHEN attempt_count >= :max THEN 'failed' ELSE 'pending' END,
                   available_at = CASE WHEN attempt_count >= :max THEN available_at ELSE :now END,
                   lease_until = NULL,
                   last_error = CASE
                     WHEN attempt_count >= :max THEN coalesce(last_error, 'LEASE_EXPIRED_AFTER_MAX_ATTEMPTS')
                     ELSE last_error
                   END
             WHERE status = 'processing'
               AND lease_until < :now
            """,
            {"max": MAX_FLUSH_ATTEMPTS, "now": now},
        )
        return recovered.rowcount

    async def recover_expired_buffer_leases(self) -> int:
        return await self._run(self._recover_expired_leases, write=True)

    async def claim_buffer_batch(self, batch_size: int, threshold: int, force: bool, lease_seconds: int) -> list[Row]:
        def operation(conn: sqlite3.Connection, now: str) -> list[Row]:
            if batch_size <= 0 or threshold <= 0 or lease_seconds <= 0:
                raise DomainError.of("INVALID_INPUT")
            # BEGIN IMMEDIATE already serializes this with every other writer, so a
            # manual flush and the worker cannot interleave claims out of seq order.
            self._recover_expired_leases(conn, now)

            if not force:
                eligible = conn.execute(
                    "SELECT count(*) FROM buffer_events WHERE status = 'pending' AND available_at <= ?",
                    (now,),
                ).fetchone()[0]
                if eligible < threshold:
                    return []

            # Reclaim the oldest incomplete flush with its original ID and timestamp,
            # so a crash after upload but before finalization retries the same key.
            existing = conn.execute(
                "SELECT flush_id, flush_timestamp FROM buffer_events"
                " WHERE status = 'pending' AND available_at <= ? AND flush_id IS NOT NULL"
                " ORDER BY seq LIMIT 1",
                (now,),
            ).fetchone()
            if existing:
                flush_id, flush_timestamp = existing["flush_id"], existing["flush_timestamp"]
                candidates = conn.execute(
                    "SELECT seq FROM buffer_events WHERE status = 'pending' AND available_at <= ? AND flush_id = ?"
                    " ORDER BY seq LIMIT ?",
                    (now, flush_id, batch_size),
                ).fetchall()
            else:
                flush_id, flush_timestamp = str(uuid4()), now
                candidates = conn.execute(
                    "SELECT seq FROM buffer_events WHERE status = 'pending' AND available_at <= ? AND flush_id IS NULL"
                    " ORDER BY seq LIMIT ?",
                    (now, batch_size),
                ).fetchall()
            seqs = [row["seq"] for row in candidates]
            if not seqs:
                return []

            lease_until = shift(now, lease_seconds)
            placeholders = ",".join("?" for _ in seqs)
            conn.execute(
                "UPDATE buffer_events SET status = 'processing', lease_until = ?, flush_id = ?, flush_timestamp = ?"
                f" WHERE seq IN ({placeholders})",
                (lease_until, flush_id, flush_timestamp, *seqs),
            )
            rows = conn.execute(f"SELECT * FROM buffer_events WHERE seq IN ({placeholders}) ORDER BY seq", seqs)
            return [row for row in (decode_row("buffer_events", item) for item in rows) if row is not None]

        return await self._run(operation, write=True)

    async def mark_buffer_flushed(self, flush_id: str, object_key: str, flushed_at: datetime | None) -> int:
        def operation(conn: sqlite3.Connection, now: str) -> int:
            if not flush_id or not (object_key or "").strip():
                raise DomainError.of("INVALID_INPUT")
            updated = conn.execute(
                "UPDATE buffer_events SET status = 'flushed', object_key = ?, flushed_at = ?,"
                " lease_until = NULL, last_error = NULL"
                " WHERE flush_id = ? AND status = 'processing'",
                (object_key, format_timestamp(flushed_at) if flushed_at else now, flush_id),
            )
            return updated.rowcount

        return await self._run(operation, write=True)

    async def mark_buffer_retry(self, flush_id: str, error: str, backoff_seconds: int) -> dict[str, int]:
        def operation(conn: sqlite3.Connection, now: str) -> dict[str, int]:
            if not flush_id or backoff_seconds < 0:
                raise DomainError.of("INVALID_INPUT")
            attempts = [
                row["attempt_count"]
                for row in conn.execute(
                    "SELECT attempt_count FROM buffer_events WHERE flush_id = ? AND status = 'processing'",
                    (flush_id,),
                )
            ]
            failed = sum(1 for attempt in attempts if attempt + 1 >= MAX_FLUSH_ATTEMPTS)
            delay = min(max(backoff_seconds, 0), MAX_BACKOFF_SECONDS)
            retry_at = shift(now, delay)
            conn.execute(
                """
                UPDATE buffer_events
                   SET attempt_count = attempt_count + 1,
                       status = CASE WHEN attempt_count + 1 >= :max THEN 'failed' ELSE 'pending' END,
                       available_at = CASE WHEN attempt_count + 1 >= :max THEN available_at ELSE :retry_at END,
                       lease_until = NULL,
                       last_error = substr(coalesce(:error, 'BUFFER_FLUSH_FAILED'), 1, :max_error)
                 WHERE flush_id = :flush_id
                   AND status = 'processing'
                """,
                {
                    "max": MAX_FLUSH_ATTEMPTS,
                    "retry_at": retry_at,
                    "error": error,
                    "max_error": MAX_ERROR_LENGTH,
                    "flush_id": flush_id,
                },
            )
            return {"retried": len(attempts) - failed, "failed": failed}

        return await self._run(operation, write=True)

    # --------------------------------------------------------- backup / restore

    async def snapshot_tables(self, tables: dict[str, str]) -> dict[str, list[Row]]:
        for table, order in tables.items():
            if table not in RESTORABLE_TABLES or not ORDER_BY_PATTERN.match(order):
                raise ValueError(f"table {table!r} cannot be backed up with order {order!r}")

        def operation(conn: sqlite3.Connection, _: str) -> dict[str, list[Row]]:
            # One read transaction: in WAL mode every SELECT below sees the same
            # snapshot, so the backup is consistent across tables.
            return {
                table: [row for row in (decode_row(table, item) for item in conn.execute(
                    f"SELECT * FROM {table} ORDER BY {order}"
                )) if row is not None]
                for table, order in tables.items()
            }

        return await self._run(operation, write=False)

    @staticmethod
    def _upsert(conn: sqlite3.Connection, table: str, rows: list[Row]) -> None:
        if not rows:
            return
        info = conn.execute(f"PRAGMA table_info({table})").fetchall()
        known = [column["name"] for column in info]
        primary_key = [column["name"] for column in sorted(info, key=lambda c: c["pk"]) if column["pk"]]
        for row in rows:
            # Column names come from CSV headers; never interpolate one the schema doesn't define.
            unknown = set(row) - set(known)
            if unknown:
                raise ValueError(f"unknown columns for {table}: {sorted(unknown)}")
            columns = [column for column in known if column in row]
            values = [
                encode_json(row[column]) if column in JSON_COLUMNS.get(table, set()) else row[column]
                for column in columns
            ]
            updates = [column for column in columns if column not in primary_key]
            conflict = (
                "DO UPDATE SET " + ", ".join(f"{column} = excluded.{column}" for column in updates)
                if updates
                else "DO NOTHING"
            )
            conn.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
                f" ON CONFLICT ({', '.join(primary_key)}) {conflict}",
                values,
            )

    async def restore_tables(self, table_rows: dict[str, list[Row]], runtime_current: list[Row]) -> None:
        unknown = set(table_rows) - set(RESTORABLE_TABLES)
        if unknown:
            raise ValueError(f"tables cannot be restored: {sorted(unknown)}")

        def operation(conn: sqlite3.Connection, _: str) -> None:
            for table in RESTORABLE_TABLES:
                if table in table_rows:
                    self._upsert(conn, table, table_rows[table])
            self._upsert(conn, "agent_runtime_current", runtime_current)

        await self._run(operation, write=True)
