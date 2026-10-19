"""Repository contract: the transactional guarantees every adapter must provide.

Every test takes the `repo` fixture, so another Repository adapter can be held
to the same contract by parametrizing that fixture.
"""

from __future__ import annotations

import asyncio
import sqlite3
from datetime import timedelta

import pytest

from app.adapters.sqlite import SqliteRepository
from app.clock import ManualClock, format_timestamp
from app.errors import DomainError


pytestmark = pytest.mark.anyio


def buffer_rows(repo: SqliteRepository) -> list[sqlite3.Row]:
    conn = sqlite3.connect(repo.path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute("SELECT * FROM buffer_events ORDER BY seq").fetchall()
    finally:
        conn.close()


async def enqueue(repo: SqliteRepository, count: int) -> list[int]:
    return [
        (await repo.enqueue_buffer_event("test.event", {"index": index}, None))["seq"]
        for index in range(count)
    ]


# --------------------------------------------------------------- agents / tasks


async def test_concurrent_task_creation_claims_the_agent_exactly_once(repo: SqliteRepository):
    agent = await repo.create_agent("openai", [], {})

    results = await asyncio.gather(
        *(repo.create_task(agent["aid"], {"n": n}) for n in range(16)),
        return_exceptions=True,
    )

    successes = [result for result in results if isinstance(result, dict)]
    failures = [result for result in results if isinstance(result, DomainError)]
    assert len(successes) == 1
    assert len(failures) == 15 and all(error.code == "AGENT_BUSY" for error in failures)
    assert len(await repo.list_tasks(agent["aid"], None, None)) == 1, "losing claims must not leave tasks behind"


async def test_failed_claim_rolls_back_every_write_in_the_transaction(repo: SqliteRepository):
    agent = await repo.create_agent("openai", [], {})
    await repo.create_task(agent["aid"], {})
    before = await repo.snapshot_tables({"tasks": "task_id", "agent_runtime_log": "timestamp, rowid"})

    with pytest.raises(DomainError, match="AGENT_BUSY"):
        await repo.create_task(agent["aid"], {})

    assert await repo.snapshot_tables({"tasks": "task_id", "agent_runtime_log": "timestamp, rowid"}) == before


async def test_migrations_are_recorded_and_idempotent(repo: SqliteRepository):
    reopened = SqliteRepository(repo.path)
    conn = sqlite3.connect(reopened.path)
    try:
        versions = [row[0] for row in conn.execute("SELECT version FROM schema_migrations ORDER BY version")]
        journal_mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()
    assert versions == ["0001_backend_api", "0002_buffer_events"]
    assert journal_mode == "wal"


# ------------------------------------------------------------------------ buffer


async def test_enqueue_assigns_monotonic_sequence_and_rejects_null_payload(repo: SqliteRepository):
    seqs = await enqueue(repo, 3)
    assert seqs == sorted(seqs) and len(set(seqs)) == 3

    with pytest.raises(DomainError, match="INVALID_INPUT"):
        await repo.enqueue_buffer_event("test.event", None, None)
    with pytest.raises(DomainError, match="INVALID_INPUT"):
        await repo.enqueue_buffer_event("   ", {}, None)


async def test_claim_respects_threshold_unless_forced(repo: SqliteRepository):
    await enqueue(repo, 3)

    assert await repo.claim_buffer_batch(100, 4, False, 300) == []
    claimed = await repo.claim_buffer_batch(100, 4, True, 300)

    assert [event["payload"]["index"] for event in claimed] == [0, 1, 2]
    assert len({event["flush_id"] for event in claimed}) == 1
    assert await repo.get_buffer_status() == {"pending": 0, "processing": 3, "flushed": 0, "failed": 0}


async def test_claim_rejects_non_positive_parameters(repo: SqliteRepository):
    for args in ((0, 1, True, 1), (1, 0, True, 1), (1, 1, True, 0)):
        with pytest.raises(DomainError, match="INVALID_INPUT"):
            await repo.claim_buffer_batch(*args)


async def test_concurrent_claims_take_disjoint_batches_in_global_order(repo: SqliteRepository):
    seqs = await enqueue(repo, 50)

    batches = await asyncio.gather(*(repo.claim_buffer_batch(10, 1, False, 300) for _ in range(5)))

    claimed = [[event["seq"] for event in batch] for batch in batches]
    assert sorted(seq for batch in claimed for seq in batch) == seqs, "every event claimed exactly once"
    assert all(batch == sorted(batch) for batch in claimed)
    ranges = sorted((batch[0], batch[-1]) for batch in claimed)
    assert all(previous[1] < current[0] for previous, current in zip(ranges, ranges[1:])), "batches never interleave"


async def test_expired_lease_is_reclaimed_with_the_same_flush_identity(repo: SqliteRepository, clock: ManualClock):
    await enqueue(repo, 2)
    first = await repo.claim_buffer_batch(100, 1, True, 30)

    clock.advance(10)
    assert await repo.claim_buffer_batch(100, 1, True, 30) == [], "a live lease is not stolen"

    clock.advance(21)
    second = await repo.claim_buffer_batch(100, 1, True, 30)

    assert [event["seq"] for event in second] == [event["seq"] for event in first]
    assert {event["flush_id"] for event in second} == {first[0]["flush_id"]}
    assert {event["flush_timestamp"] for event in second} == {first[0]["flush_timestamp"]}
    assert all(event["attempt_count"] == 0 for event in second), "lease recovery alone does not count an attempt"


async def test_retry_applies_bounded_backoff(repo: SqliteRepository, clock: ManualClock):
    await enqueue(repo, 1)
    claimed = await repo.claim_buffer_batch(100, 1, True, 300)

    result = await repo.mark_buffer_retry(claimed[0]["flush_id"], "boom", 4)
    assert result == {"retried": 1, "failed": 0}
    assert await repo.claim_buffer_batch(100, 1, True, 300) == []
    clock.advance(4)
    again = await repo.claim_buffer_batch(100, 1, True, 300)
    assert again[0]["attempt_count"] == 1
    assert again[0]["flush_id"] == claimed[0]["flush_id"]

    await repo.mark_buffer_retry(again[0]["flush_id"], "boom", 10_000)
    assert buffer_rows(repo)[0]["available_at"] == format_timestamp(clock.now() + timedelta(seconds=300))
    clock.advance(299)
    assert await repo.claim_buffer_batch(100, 1, True, 300) == []
    clock.advance(1)
    assert await repo.claim_buffer_batch(100, 1, True, 300) != [], "backoff is capped at 300 seconds"


async def test_fifth_failure_is_terminal_and_does_not_block_newer_events(repo: SqliteRepository):
    await enqueue(repo, 1)
    for attempt in range(5):
        claimed = await repo.claim_buffer_batch(100, 1, True, 300)
        assert claimed and claimed[0]["attempt_count"] == attempt
        result = await repo.mark_buffer_retry(claimed[0]["flush_id"], "x" * 5000, 0)
    assert result == {"retried": 0, "failed": 1}

    poison = buffer_rows(repo)[0]
    assert poison["status"] == "failed"
    assert len(poison["last_error"]) == 2000

    await enqueue(repo, 1)
    fresh = await repo.claim_buffer_batch(100, 1, True, 300)
    assert [event["payload"]["index"] for event in fresh] == [0]
    assert fresh[0]["seq"] > poison["seq"]


async def test_expired_lease_after_max_attempts_becomes_failed(repo: SqliteRepository):
    await enqueue(repo, 1)
    for _ in range(4):
        claimed = await repo.claim_buffer_batch(100, 1, True, 300)
        await repo.mark_buffer_retry(claimed[0]["flush_id"], "boom", 0)
    claimed = await repo.claim_buffer_batch(100, 1, True, 300)
    await repo.mark_buffer_retry(claimed[0]["flush_id"], "boom", 0)
    assert buffer_rows(repo)[0]["status"] == "failed"

    # A batch that reached the limit and then lost its lease is also retired.
    conn = sqlite3.connect(repo.path)
    try:
        conn.execute(
            "UPDATE buffer_events SET status = 'processing', lease_until = ?, flush_timestamp = ?, last_error = NULL",
            ("2000-01-01T00:00:00.000000+00:00", "2000-01-01T00:00:00.000000+00:00"),
        )
        conn.commit()
    finally:
        conn.close()
    assert await repo.recover_expired_buffer_leases() == 1
    row = buffer_rows(repo)[0]
    assert (row["status"], row["last_error"]) == ("failed", "LEASE_EXPIRED_AFTER_MAX_ATTEMPTS")


async def test_finalization_only_touches_the_processing_rows_of_its_flush(repo: SqliteRepository):
    await enqueue(repo, 3)
    first = await repo.claim_buffer_batch(2, 1, True, 300)
    second = await repo.claim_buffer_batch(2, 1, True, 300)

    assert await repo.mark_buffer_flushed(first[0]["flush_id"], "key-1", None) == 2
    assert await repo.mark_buffer_flushed(first[0]["flush_id"], "key-1", None) == 0, "already flushed"
    assert await repo.get_buffer_status() == {"pending": 0, "processing": 1, "flushed": 2, "failed": 0}
    assert second[0]["flush_id"] != first[0]["flush_id"]


# ----------------------------------------------------------------- restore


async def test_restore_rejects_columns_the_schema_does_not_define(repo: SqliteRepository):
    with pytest.raises(ValueError, match="unknown columns"):
        await repo.restore_tables({"agent_skills": [{"aid": "a", "skill": "s", "x); DROP TABLE tasks; --": 1}]}, [])


async def test_restore_is_all_or_nothing(repo: SqliteRepository):
    agent = {
        "aid": "11111111-1111-4111-8111-111111111111",
        "model_provider": "openai",
        "llm_config": {},
        "status": "active",
        "created_at": "2026-08-26T00:00:00.000000+00:00",
        "updated_at": "2026-08-26T00:00:00.000000+00:00",
        "disabled_at": None,
    }
    orphan_skill = {"aid": "22222222-2222-4222-8222-222222222222", "skill": "planner"}

    with pytest.raises(DomainError) as failure:
        await repo.restore_tables({"agent_registry": [agent], "agent_skills": [orphan_skill]}, [])

    assert (failure.value.status_code, failure.value.code) == (409, "DATABASE_ERROR")
    assert await repo.list_agents(None, []) == [], "the agent row must roll back with the failing skill row"
