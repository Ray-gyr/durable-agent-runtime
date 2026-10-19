"""Buffer flush, backup and restore against the real local adapters.

Failures are injected at the two boundaries the design has to survive:
the object store (before write, or after write with a lost acknowledgement) and
the database finalization that follows a successful upload.
"""

from __future__ import annotations

import csv
import io
import os
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from httpx import AsyncClient

from app.adapters.faulty_store import FaultPlan, FaultyObjectStore
from app.adapters.local_store import LocalObjectStore
from app.adapters.sqlite import SqliteRepository
from app.buffer_service import BACKUP_TABLES, create_backup, events_to_csv, flush_once, validate_manifest
from app.buffer_worker import run_worker
from app.clock import ManualClock
from app.config import Settings
from app.ports import ObjectNotFound
from app.restore_backup import restore_from_manifest


pytestmark = pytest.mark.anyio

MISSING = object()


async def flush(repo, store, *, force: bool = True, threshold: int = 100) -> dict[str, Any]:
    return await flush_once(repo, store, batch_size=100, threshold=threshold, force=force, lease_seconds=300)


async def enqueue(repo: SqliteRepository, count: int) -> None:
    for index in range(count):
        await repo.enqueue_buffer_event("test.event", {"index": index, "text": "quoted, \"value\" café ✓"}, None)


def objects(store: LocalObjectStore, category: str = "buffer") -> list[Path]:
    return sorted(path for path in (store.root / "test" / category).rglob("*") if path.is_file())


def event_rows(repo: SqliteRepository) -> list[sqlite3.Row]:
    conn = sqlite3.connect(repo.path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute("SELECT * FROM buffer_events ORDER BY seq").fetchall()
    finally:
        conn.close()


class FinalizationFailsOnce(SqliteRepository):
    """The database update after a successful upload fails the first time."""

    failures = 1

    async def mark_buffer_flushed(self, flush_id: str, object_key: str, flushed_at: datetime | None) -> int:
        if self.failures:
            self.failures -= 1
            raise RuntimeError("database finalization unavailable")
        return await super().mark_buffer_flushed(flush_id, object_key, flushed_at)


class RecordingStore(LocalObjectStore):
    def __init__(self, root: Path, prefix: str) -> None:
        super().__init__(root, prefix)
        self.put_keys: list[str] = []

    async def put(self, key: str, body: bytes, content_type: str) -> bool:
        self.put_keys.append(key)
        return await super().put(key, body, content_type)


# --------------------------------------------------------------------- flush


def test_events_to_csv_sorts_by_global_seq_and_escapes_json():
    first = {"seq": 2, "event_id": "b", "event_type": "t", "payload": {"message": "quoted, value"}}
    second = {"seq": 1, "event_id": "a", "event_type": "t", "payload": [1, "two"]}
    rows = list(csv.DictReader(io.StringIO(events_to_csv([first, second]).decode("utf-8"))))
    assert [row["seq"] for row in rows] == ["1", "2"]
    assert rows[0]["payload_json"] == '[1,"two"]'
    assert rows[1]["payload_json"] == '{"message":"quoted, value"}'


async def test_flush_uploads_csv_then_marks_events_flushed(repo: SqliteRepository, store: LocalObjectStore):
    await enqueue(repo, 3)

    result = await flush(repo, store)

    assert (result["status"], result["event_count"]) == ("flushed", 3)
    assert result["object_key"].startswith("test/buffer/2026-08-26/flush-20260826T000102000000Z-")
    body = await store.get(result["object_key"])
    rows = list(csv.DictReader(io.StringIO(body.decode("utf-8"))))
    assert [row["payload_json"] for row in rows][0] == '{"index":0,"text":"quoted, \\"value\\" café ✓"}'
    assert [int(row["seq"]) for row in rows] == [1, 2, 3]
    assert {(row["status"], row["object_key"]) for row in event_rows(repo)} == {("flushed", result["object_key"])}


async def test_automatic_flush_waits_for_the_threshold(repo: SqliteRepository, store: LocalObjectStore):
    await enqueue(repo, 2)
    assert (await flush(repo, store, force=False, threshold=3))["status"] == "idle"
    await enqueue(repo, 1)
    assert (await flush(repo, store, force=False, threshold=3))["event_count"] == 3


async def test_store_failure_schedules_a_retry_with_backoff(
    repo: SqliteRepository, store: LocalObjectStore, clock: ManualClock
):
    await enqueue(repo, 1)
    faulty = FaultyObjectStore(store, FaultPlan(put_failures=1))

    first = await flush(repo, faulty)
    assert (first["status"], first["retry"]) == ("retrying", {"retried": 1, "failed": 0})
    assert objects(store) == []
    row = event_rows(repo)[0]
    assert (row["status"], row["attempt_count"], row["last_error"]) == (
        "pending",
        1,
        f"injected put failure for {first['object_key']}",
    )

    assert (await flush(repo, faulty))["status"] == "idle", "backoff hides the event for one second"
    clock.advance(1)
    second = await flush(repo, faulty)
    assert (second["status"], second["object_key"]) == ("flushed", first["object_key"])


@pytest.mark.parametrize("failure", ["finalization", "lost_ack"])
async def test_retry_after_successful_upload_reuses_the_key_without_duplicating(
    tmp_path: Path, store: LocalObjectStore, clock: ManualClock, failure: str
):
    if failure == "finalization":
        repo: SqliteRepository = FinalizationFailsOnce(tmp_path / "f.db", clock=clock)
        target: Any = store
    else:
        repo = SqliteRepository(tmp_path / "f.db", clock=clock)
        target = FaultyObjectStore(store, FaultPlan(lost_acks=1))
    await enqueue(repo, 2)

    first = await flush(repo, target)
    assert first["status"] == "retrying"
    assert len(objects(store)) == 1, "the object was written before the failure"

    clock.advance(1)
    second = await flush(repo, target)

    assert (second["status"], second["event_count"]) == ("flushed", 2)
    assert second["object_key"] == first["object_key"]
    assert second["flush_id"] == first["flush_id"]
    assert len(objects(store)) == 1


async def test_crashed_worker_batch_is_recovered_after_lease_expiry(
    repo: SqliteRepository, store: LocalObjectStore, clock: ManualClock
):
    await enqueue(repo, 2)
    abandoned = await repo.claim_buffer_batch(100, 1, True, 300)  # the worker dies here

    assert (await flush(repo, store))["status"] == "idle"
    clock.advance(301)
    recovered = await flush(repo, store)

    assert (recovered["status"], recovered["flush_id"]) == ("flushed", abandoned[0]["flush_id"])


async def test_poison_batch_fails_after_five_attempts_and_newer_events_still_flush(
    repo: SqliteRepository, store: LocalObjectStore, clock: ManualClock
):
    await enqueue(repo, 1)
    faulty = FaultyObjectStore(store, FaultPlan(put_failures=5))
    statuses = []
    for _ in range(5):
        statuses.append((await flush(repo, faulty))["status"])
        clock.advance(300)
    assert statuses == ["retrying", "retrying", "retrying", "retrying", "failed"]

    await enqueue(repo, 1)
    newer = await flush(repo, faulty)
    assert (newer["status"], newer["event_count"]) == ("flushed", 1)
    assert await repo.get_buffer_status() == {"pending": 0, "processing": 0, "flushed": 1, "failed": 1}


async def test_worker_once_flushes_only_at_threshold(
    repo: SqliteRepository, store: LocalObjectStore, settings: Settings, capsys: pytest.CaptureFixture[str]
):
    await enqueue(repo, 99)
    await run_worker(repo, store, settings, once=True)
    assert capsys.readouterr().out == ""
    await enqueue(repo, 1)
    await run_worker(repo, store, settings, once=True)
    assert '"status": "flushed"' in capsys.readouterr().out
    assert len(objects(store)) == 1


# ---------------------------------------------------------- backup / restore


async def populate(repo: SqliteRepository, clock: ManualClock) -> None:
    finished = await repo.create_agent("openai", ["planner", "researcher"], {"temperature": 0, "tags": ["a,b"]})
    task = await repo.create_task(finished["aid"], {"prompt": "line\nbreak"})
    clock.advance(1)
    await repo.record_task_result(task["task_id"], {"answer": "café ✓"}, "completed")

    busy = await repo.create_agent("anthropic", [], {})
    await repo.create_task(busy["aid"], {})

    retired = await repo.create_agent("local", ["planner"], {})
    await repo.disable_agent(retired["aid"])


@pytest.fixture
def fresh_repo(tmp_path: Path, clock: ManualClock) -> SqliteRepository:
    return SqliteRepository(tmp_path / "restored.db", clock=clock)


async def test_backup_writes_every_table_then_the_manifest_last(
    repo: SqliteRepository, tmp_path: Path, clock: ManualClock
):
    await populate(repo, clock)
    store = RecordingStore(tmp_path / "objects", "test")

    manifest = await create_backup(repo, store)

    assert manifest["status"] == "completed"
    assert validate_manifest(manifest) == []
    assert store.put_keys[-1] == manifest["manifest_key"]
    assert [key.rsplit("/", 1)[-1] for key in store.put_keys] == [f"{table}.csv" for table in BACKUP_TABLES] + [
        "manifest.json"
    ]
    assert {item["table"]: item["row_count"] for item in manifest["objects"]} == {
        "agent_registry": 3,
        "agent_skills": 3,
        "tasks": 2,
        "agent_runtime_log": 4,
    }


async def test_restore_round_trip_reproduces_every_table(
    repo: SqliteRepository, fresh_repo: SqliteRepository, store: LocalObjectStore, clock: ManualClock
):
    await populate(repo, clock)
    manifest = await create_backup(repo, store)

    result = await restore_from_manifest(manifest["manifest_key"], apply=True, repo=fresh_repo, store=store)

    tables = {**BACKUP_TABLES, "agent_runtime_current": "aid"}
    assert result["status"] == "applied"
    assert await fresh_repo.snapshot_tables(tables) == await repo.snapshot_tables(tables)


async def test_restore_rebuilds_current_state_for_an_agent_that_finished_a_task(
    repo: SqliteRepository, fresh_repo: SqliteRepository, store: LocalObjectStore
):
    """Completing a task logs `idle` with its task_id; restore must not copy that into an idle current row."""
    agent = await repo.create_agent("openai", [], {})
    task = await repo.create_task(agent["aid"], {})
    await repo.record_task_result(task["task_id"], {}, "completed")
    manifest = await create_backup(repo, store)

    await restore_from_manifest(manifest["manifest_key"], apply=True, repo=fresh_repo, store=store)

    current = (await fresh_repo.snapshot_tables({"agent_runtime_current": "aid"}))["agent_runtime_current"]
    assert [(row["state"], row["task_id"]) for row in current] == [("idle", None)]
    assert (await fresh_repo.create_task(agent["aid"], {}))["status"] == "pending", "restored agent is claimable"


async def test_restore_validate_only_reads_everything_and_writes_nothing(
    repo: SqliteRepository, fresh_repo: SqliteRepository, store: LocalObjectStore, clock: ManualClock
):
    await populate(repo, clock)
    manifest = await create_backup(repo, store)

    result = await restore_from_manifest(manifest["manifest_key"], apply=False, repo=fresh_repo, store=store)

    assert result["status"] == "validated"
    assert result["rows"] == {item["table"]: item["row_count"] for item in manifest["objects"]}
    assert await fresh_repo.list_agents(None, []) == []


async def test_restore_rejects_a_corrupted_object_before_any_write(
    repo: SqliteRepository, fresh_repo: SqliteRepository, store: LocalObjectStore, clock: ManualClock
):
    await populate(repo, clock)
    manifest = await create_backup(repo, store)
    tasks_key = next(item["key"] for item in manifest["objects"] if item["table"] == "tasks")
    store.path_for(tasks_key).write_bytes(b"corrupted")

    with pytest.raises(ValueError, match="Checksum mismatch for tasks"):
        await restore_from_manifest(manifest["manifest_key"], apply=True, repo=fresh_repo, store=store)
    assert await fresh_repo.list_agents(None, []) == []


async def test_restore_with_missing_manifest_writes_nothing(fresh_repo: SqliteRepository, store: LocalObjectStore):
    with pytest.raises(ObjectNotFound):
        await restore_from_manifest("test/backups/missing/manifest.json", apply=True, repo=fresh_repo, store=store)
    assert await fresh_repo.list_agents(None, []) == []


# ----------------------------------------------------------------------- API


@pytest.fixture
def debug_token() -> Iterator[str]:
    previous = os.environ.get("BUFFER_DEBUG_TOKEN", MISSING)
    os.environ["BUFFER_DEBUG_TOKEN"] = "local-debug-token"
    yield "local-debug-token"
    if previous is MISSING:
        os.environ.pop("BUFFER_DEBUG_TOKEN", None)
    else:
        os.environ["BUFFER_DEBUG_TOKEN"] = str(previous)


async def test_buffer_endpoints_enqueue_status_and_protect_flush_and_backup(api: AsyncClient, debug_token: str):
    created = await api.post("/v1/buffer/events", json={"event_type": " test ", "payload": {"ok": True}})
    status_result = await api.get("/v1/buffer/status")
    unauthorized = await api.post("/v1/buffer/flush", headers={"X-Buffer-Debug-Token": "wrong"})
    authorized = await api.post("/v1/buffer/flush", headers={"X-Buffer-Debug-Token": debug_token})
    backup = await api.post("/v1/backups", headers={"X-Buffer-Debug-Token": debug_token})

    assert created.status_code == 201
    assert set(created.json()) == {"event_id", "seq"} and created.json()["seq"] == 1
    assert status_result.json() == {"pending": 1, "processing": 0, "flushed": 0, "failed": 0}
    assert (unauthorized.status_code, unauthorized.json()["error_code"]) == (401, "BUFFER_DEBUG_UNAUTHORIZED")
    assert (authorized.status_code, authorized.json()["status"]) == (200, "flushed")
    assert (backup.status_code, backup.json()["status"]) == (200, "completed")


async def test_null_payload_is_rejected(api: AsyncClient):
    result = await api.post("/v1/buffer/events", json={"event_type": "test", "payload": None})
    assert (result.status_code, result.json()["error_code"]) == (422, "INVALID_INPUT")


async def test_debug_endpoints_are_closed_when_no_token_is_configured(api: AsyncClient):
    previous = os.environ.pop("BUFFER_DEBUG_TOKEN", None)
    try:
        result = await api.post("/v1/buffer/flush", headers={"X-Buffer-Debug-Token": ""})
    finally:
        if previous is not None:
            os.environ["BUFFER_DEBUG_TOKEN"] = previous
    assert (result.status_code, result.json()["error_code"]) == (503, "BUFFER_DEBUG_NOT_CONFIGURED")
